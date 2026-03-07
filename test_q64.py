import sys
import fitz
from parser.block_extractor import PyMuPDFExtractor
from parser.state_machine import ParserStateMachine

doc = fitz.open(sys.argv[1])
extractor = PyMuPDFExtractor(doc)
blocks = extractor.extract(start_page=30, end_page=31)

state_machine = ParserStateMachine()
questions = state_machine.parse(blocks)

for q in questions:
    if q.question_number == 64:
        print(f"Option keys: {[opt.key for opt in q.options]}")
        print(f"Answer text: {q.answer_text}")
        print(f"Options correct: {[opt.is_correct for opt in q.options]}")
