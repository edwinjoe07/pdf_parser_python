"""Debug: validate rendering filter produces correct results."""
import json

data = json.load(open('output/000dba9f-2ed0-4efd-8a5e-1c623184dca1_parsed.json', encoding='utf-8'))

MARGIN = 10
MAX_SECTION_IMAGES = 15

def get_filtered_images(blocks):
    """Mirror the JS getFilteredSectionImages logic (per-text-block proximity)."""
    images = [b for b in blocks if b['type'] == 'image']
    if not images:
        return []
    text_indices = [b['order_index'] for b in blocks if b['type'] == 'text' and isinstance(b.get('order_index'), (int, float))]
    if text_indices:
        filtered = [img for img in images
                    if any(abs(img.get('order_index', 0) - ti) <= MARGIN for ti in text_indices)]
    else:
        filtered = images
    # Dedup
    seen = set()
    deduped = []
    for img in filtered:
        key = img['content'].replace('\\', '/')
        if key not in seen:
            seen.add(key)
            deduped.append(img)
    return deduped[:MAX_SECTION_IMAGES]

# Test Q13
q13 = next(q for q in data['questions'] if q['question_number'] == 13)
print(f"=== Q13 (pages {q13['page_start']}-{q13['page_end']}) ===")
for sec in ['question', 'options', 'answer', 'explanation']:
    blocks = q13['blocks'].get(sec, [])
    raw_imgs = [b for b in blocks if b['type'] == 'image']
    filtered = get_filtered_images(blocks)
    print(f"  [{sec}] raw: {len(raw_imgs)} images -> filtered: {len(filtered)} images")
    for img in filtered[:5]:
        print(f"    {img['content']}")

# Test Q2
print()
q2 = next(q for q in data['questions'] if q['question_number'] == 2)
print(f"=== Q2 (pages {q2['page_start']}-{q2['page_end']}) ===")
for sec in ['question', 'options', 'answer', 'explanation']:
    blocks = q2['blocks'].get(sec, [])
    raw_imgs = [b for b in blocks if b['type'] == 'image']
    filtered = get_filtered_images(blocks)
    print(f"  [{sec}] raw: {len(raw_imgs)} images -> filtered: {len(filtered)} images")
    for img in filtered[:5]:
        print(f"    {img['content']}")

# Test Q3
print()
q3 = next(q for q in data['questions'] if q['question_number'] == 3)
print(f"=== Q3 (pages {q3['page_start']}-{q3['page_end']}) ===")
for sec in ['question', 'options', 'answer', 'explanation']:
    blocks = q3['blocks'].get(sec, [])
    raw_imgs = [b for b in blocks if b['type'] == 'image']
    filtered = get_filtered_images(blocks)
    print(f"  [{sec}] raw: {len(raw_imgs)} images -> filtered: {len(filtered)} images")
    for img in filtered[:5]:
        print(f"    {img['content']}")

# Test Q6 (multi-section images)
print()
q6 = next(q for q in data['questions'] if q['question_number'] == 6)
print(f"=== Q6 (pages {q6['page_start']}-{q6['page_end']}) ===")
for sec in ['question', 'options', 'answer', 'explanation']:
    blocks = q6['blocks'].get(sec, [])
    raw_imgs = [b for b in blocks if b['type'] == 'image']
    filtered = get_filtered_images(blocks)
    print(f"  [{sec}] raw: {len(raw_imgs)} images -> filtered: {len(filtered)} images")
    for img in filtered[:5]:
        print(f"    {img['content']}")

# Summary across all questions
print()
print("=== SUMMARY: image counts before/after filtering ===")
total_raw = 0
total_filtered = 0
max_filtered = 0
for q in data['questions']:
    for sec, blocks in q['blocks'].items():
        raw = len([b for b in blocks if b['type'] == 'image'])
        filt = len(get_filtered_images(blocks))
        total_raw += raw
        total_filtered += filt
        max_filtered = max(max_filtered, filt)
print(f"  Total raw image refs:     {total_raw}")
print(f"  Total filtered image refs: {total_filtered}")
print(f"  Max filtered in one section: {max_filtered}")
print(f"  Reduction: {((1 - total_filtered/total_raw)*100):.1f}%" if total_raw > 0 else "  No images")
