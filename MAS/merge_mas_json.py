import json

# Load the two JSON files
with open('./MAS_first_1500.json', 'r', encoding='utf-8') as f1, \
     open('./MAS_last_1500.json', 'r', encoding='utf-8') as f2:
    data1 = json.load(f1)
    data2 = json.load(f2)

# Merge and deduplicate based on a canonical field
# Use node id and optionally node name as deduplication keys
seen = set()
merged_nodes = []

for node in data1.get("nodes", []) + data2.get("nodes", []):
    node_id = node.get("id")
    if node_id and node_id not in seen:
        seen.add(node_id)
        merged_nodes.append(node)

# Preserve all other keys from the first file
merged_data = {**data1, "nodes": merged_nodes}

# Output the merged JSON to a new file
output_path = "./MAS_merged_nodes.json"
with open(output_path, 'w', encoding='utf-8') as out_file:
    json.dump(merged_data, out_file, indent=2, ensure_ascii=False)

print(f"Merged file saved to: {output_path}")
