
import re
from parser.state_machine import IGNORE_PATTERNS

test_string = "answer.)"

print(f"Testing string: '{test_string}'")
for i, pattern in enumerate(IGNORE_PATTERNS):
    if pattern.match(test_string):
        print(f"MATCH FOUND! Pattern index {i}: {pattern.pattern}")
    else:
        # print(f"No match for pattern {i}")
        pass
