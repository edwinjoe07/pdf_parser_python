"""
Block Extractor
===============
Extracts text and image blocks from PDF files using PyMuPDF (fitz).
Preserves layout information, font metadata, and exact positioning.
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF

from .models import BlockType, ContentBlock, FontInfo

logger = logging.getLogger(__name__)


class BlockExtractor:
    """
    Handles PDF ingestion and low-level block extraction.
    
    Extracts:
        - Text blocks with font metadata (bold, size, etc.)
        - Images with bounding boxes and original resolution
    """

    def __init__(
        self,
        image_output_dir: str,
        image_format: str = "png",
        min_image_size: int = 50,
        dpi: int = 150,
    ):
        self.image_output_dir = Path(image_output_dir)
        self.image_format = image_format
        self.min_image_size = min_image_size
        self.dpi = dpi
        
        self.image_output_dir.mkdir(parents=True, exist_ok=True)

    def get_page_count(self, pdf_path: str) -> int:
        """Get total number of pages in the PDF."""
        with fitz.open(pdf_path) as doc:
            return doc.page_count

    def extract(
        self,
        pdf_path: str,
        page_range: Optional[tuple[int, int]] = None,
        progress_callback: Optional[callable] = None,
    ) -> list[ContentBlock]:
        """
        Extract all content blocks from the PDF.
        
        Args:
            pdf_path: Path to the PDF file.
            page_range: Optional (start, end) range (1-indexed, inclusive).
            progress_callback: Optional callable(current, total).
            
        Returns:
            Flat list of ContentBlock objects ordered by appearance.
        """
        all_blocks: list[ContentBlock] = []
        global_order = 0

        with fitz.open(pdf_path) as doc:
            total_pages = doc.page_count
            
            # Determine page range (1-indexed)
            start_page = 1
            end_page = total_pages
            if page_range:
                start_page = max(1, page_range[0])
                end_page = min(total_pages, page_range[1])

            logger.info(
                f"Extracting blocks from {pdf_path} "
                f"(pages {start_page} to {end_page})"
            )

            for page_idx in range(start_page - 1, end_page):
                page = doc[page_idx]
                page_num = page_idx + 1
                
                # ─── 1. Extract Images First (High Quality) ────────────────────────
                page_images = self._extract_images_from_page(page, page_num, global_order)
                
                # Update global order for text blocks based on how many images were found
                # Actually, we can just append them all and sort.
                current_page_blocks = []
                current_page_blocks.extend(page_images)

                # ─── 2. Extract Text Blocks ────────────────────────────────────────
                
                # Use 'dict' format to get detailed layout info
                page_dict = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)
                
                for block in page_dict.get("blocks", []):
                    # Text Block
                    if block["type"] == 0:  # Text
                        text_content = self._process_text_block(block)
                        if text_content.strip():
                            # Extract font info from first span
                            font_info = None
                            if block.get("lines") and block["lines"][0].get("spans"):
                                span = block["lines"][0]["spans"][0]
                                font_info = FontInfo(
                                    name=span.get("font"),
                                    size=span.get("size"),
                                    flags=span.get("flags"),
                                    color=span.get("color"),
                                    is_bold=bool(span.get("flags", 0) & 2),
                                    is_italic=bool(span.get("flags", 0) & 1),
                                )

                            current_page_blocks.append(ContentBlock(
                                type=BlockType.TEXT,
                                content=text_content,
                                page_number=page_num,
                                bbox=block["bbox"],
                                order_index=global_order, # temporary
                                font_info=font_info,
                            ))
                
                # ─── 3. Final Page Sort & Indexing ─────────────────────────────────
                # Sort by vertical position (Y) primarily -> Reading Order
                current_page_blocks.sort(key=lambda x: (x.bbox[1], x.bbox[0]))
                
                # Add to main list
                all_blocks.extend(current_page_blocks)
                
                # Update global counter after page is done
                global_order += len(current_page_blocks)

                if progress_callback:
                    # Provide progress for extraction phase
                    progress_callback(page_num - start_page + 1, end_page - start_page + 1)

        # Final Re-Indexing Global Order
        for idx, b in enumerate(all_blocks):
            b.order_index = idx

        return all_blocks

    def _process_text_block(self, block: dict) -> str:
        """Combine spans in a text block into a single string."""
        lines = []
        for line in block.get("lines", []):
            line_text = "".join(span["text"] for span in line.get("spans", []))
            lines.append(line_text)
        return "\n".join(lines)

    def _extract_images_from_page(
        self, page: fitz.Page, page_num: int, start_order: int
    ) -> list[ContentBlock]:
        """Extract high-quality images from a page with xref-caching for speed."""
        image_blocks: list[ContentBlock] = []
        doc = page.parent

        # Initialize caches if not present
        if not hasattr(self, "_image_cache"):
            self._image_cache = {}  # xref -> {rel_path, is_logo}
            self._image_hashes = {} # hash -> {rel_path, count}
            self._image_counter = 0

        # Get list of images on the page
        page_images_raw = page.get_images(full=True)
        
        # Limit the number of images processed per page to avoid excessive processing
        if len(page_images_raw) > 2000:
            logger.warning(f"Page {page_num} has {len(page_images_raw)} images. Skipping to avoid overload.")
            return []

        for img_idx, img in enumerate(page_images_raw):
            xref = img[0]
            
            # Check cached xref status
            if xref in self._image_cache:
                cached = self._image_cache[xref]
                if cached.get("is_logo"):
                    continue
                
                # Image exists, but we need the bbox for THIS page instance
                rects = page.get_image_rects(xref)
                if not rects:
                    continue
                
                bbox_obj = rects[0]
                if bbox_obj.width < 1 or bbox_obj.height < 1:
                    continue

                image_blocks.append(ContentBlock(
                    type=BlockType.IMAGE,
                    content=cached["rel_path"],
                    page_number=page_num,
                    bbox=(bbox_obj.x0, bbox_obj.y0, bbox_obj.x1, bbox_obj.y1),
                    order_index=start_order + img_idx,
                ))
                continue

            # New image, extract and analyze
            try:
                base_image = doc.extract_image(xref)
                if not base_image:
                    continue
                
                width, height = base_image["width"], base_image["height"]
                # 1. Filter icons/separators by pixel size
                if width < self.min_image_size or height < self.min_image_size:
                    self._image_cache[xref] = {"is_logo": True}
                    continue

                # 2. Rendered bbox check
                rects = page.get_image_rects(xref)
                if not rects:
                    # Might be hidden on this page or not rendered
                    continue
                
                bbox_obj = rects[0]
                if bbox_obj.width < 1 or bbox_obj.height < 1:
                    continue
                
                bbox = (bbox_obj.x0, bbox_obj.y0, bbox_obj.x1, bbox_obj.y1)
                area = bbox_obj.width * bbox_obj.height

                # 3. Duplicate/Logo check via Hash
                img_data = base_image["image"]
                img_hash = hashlib.md5(img_data).hexdigest()
                
                if img_hash not in self._image_hashes:
                    self._image_hashes[img_hash] = {"rel_path": None, "count": 0}
                
                self._image_hashes[img_hash]["count"] += 1
                
                # Permissive logo filtering: if it repeats many times AND is small
                is_logo = False
                if self._image_hashes[img_hash]["count"] > 5 and area < 10000:
                    is_logo = True
                
                if is_logo:
                    self._image_cache[xref] = {"is_logo": True}
                    continue

                # 4. Save and Cache
                rel_path = self._image_hashes[img_hash]["rel_path"]
                if not rel_path:
                    # Actually save to disk
                    self._image_counter += 1
                    ext = base_image["ext"]
                    filename = f"q_img_{self._image_counter}_{xref}_{page_num}.{ext}" 
                    abs_path = self.image_output_dir / filename
                    with open(abs_path, "wb") as f:
                        f.write(img_data)
                    
                    rel_path = f"questions/{self.image_output_dir.name}/{filename}" 
                    self._image_hashes[img_hash]["rel_path"] = rel_path
                
                # Cache xref result
                self._image_cache[xref] = {"rel_path": rel_path, "is_logo": False}

                image_blocks.append(ContentBlock(
                    type=BlockType.IMAGE,
                    content=rel_path,
                    page_number=page_num,
                    bbox=bbox,
                    order_index=start_order + img_idx,
                ))

            except Exception as e:
                logger.warning(f"Failed extracting image {xref} on page {page_num}: {e}")

        return image_blocks
