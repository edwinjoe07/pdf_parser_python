
import re

ANSWER_PATTERN = re.compile(
    r"^\s*(?:Correct\s+)?(?:Answer|Ans|Key|Correct\s+Key)\b\s*(?::|\s+[A-E](?:\b|$))\s*", re.IGNORECASE
)

test_string = "answer.)"
match = ANSWER_PATTERN.match(test_string)

print(f"Testing string: '{test_string}'")
print(f"Match found: {bool(match)}")
if match:
    print(f"Matched text: '{match.group(0)}'")
