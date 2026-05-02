
import logging
from parser.state_machine import StateMachineParser
from parser.models import ContentBlock, BlockType

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def test_parser():
    parser = StateMachineParser()
    
    # Blocks from page 3 diagnostic
    blocks = [
        ContentBlock(type=BlockType.TEXT, content="Question: 10\n", page_number=3, bbox=[57.8, 383.5, 131.1, 402.3], order_index=0),
        ContentBlock(type=BlockType.TEXT, content="What is typically the biggest load on a CPU when managing encrypted traffic? (Choose the best\nanswer.)\n", page_number=3, bbox=[57.8, 417.4, 509.1, 447.0], order_index=1),
        ContentBlock(type=BlockType.TEXT, content="A. Emulating certificates\nB. Using the SHA-2 hash function\nC. Using RSA encryption\nD. The need for redirection\n", page_number=3, bbox=[57.8, 461.9, 204.4, 521.2], order_index=2),
        ContentBlock(type=BlockType.TEXT, content="Answer: A\n", page_number=3, bbox=[416.1, 536.9, 475.2, 555.7], order_index=3),
    ]
    
    questions = parser.parse(blocks)
    
    print(f"\n--- Results ---")
    print(f"Total Questions: {len(questions)}")
    for q in questions:
        print(f"Q{q.question_number} | Text: {q.question_text[:50]}...")
        print(f"Options: {[opt.key for opt in q.options]}")
        print(f"Answer: {q.answer_text}")
        print(f"Anomalies: {[a.type.value for a in q.anomalies]}")

if __name__ == "__main__":
    test_parser()
