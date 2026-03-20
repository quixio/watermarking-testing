import subprocess, json

result = subprocess.run(
    ["docker", "exec", "watermarkingtest-kafka_broker-1",
     "rpk", "topic", "consume", "colours", "--offset", "@-24h:end", "-f", "%v\n"],
    capture_output=True, text=True, timeout=120
)

run_id_filter = "run_2026-03-19 16:37:08.046610+01:00updatedWMAlgo"
totals = {}
msg_count = 0
for line in result.stdout.splitlines():
    line = line.strip()
    if line.startswith("{"):
        try:
            d = json.loads(line)
            rid = d.get("run_id", "?")
            totals[rid] = totals.get(rid, 0) + d.get("count", 0)
            msg_count += 1
        except:
            pass

print(f"Total window messages read: {msg_count:,}")
print(f"Total unique run_ids: {len(totals)}")
print("\nsum(count) per run_id:")
for k in sorted(totals):
    marker = " <-- TARGET" if k == run_id_filter else ""
    print(f"  {k[:55]}: {totals[k]:,}{marker}")

target = totals.get(run_id_filter, 0)
print(f"\nTarget run sum(count): {target:,}")
print(f"Expected:              2,000,000")
print(f"PASS: {target == 2000000}")
