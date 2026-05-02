import os
import json

output_dir = "debug_output"
files = [f for f in os.listdir(output_dir) if f.endswith("_validation.json")]

total_detected = 0
total_success = 0

print(f"{'Filename':<30} | {'Rate':<10} | {'Details'}")
print("-" * 60)

for f in files:
    with open(os.path.join(output_dir, f), encoding='utf-8') as j:
        v = json.load(j)
        rate = v['success_rate']
        success = v['structured_successfully']
        detected = v['total_questions_detected']
        
        total_detected += detected
        total_success += success
        
        print(f"{f:<30} | {rate:>9.2f}% | {success}/{detected}")

overall_rate = (total_success / total_detected * 100) if total_detected > 0 else 0
print("-" * 60)
print(f"{'OVERALL':<30} | {overall_rate:>9.2f}% | {total_success}/{total_detected}")
