import os
import json

# Define the output file name
output_file_name = "processed_gabon_data.json"

# Get the current working directory
current_directory = os.getcwd()

# List to store all raw data from JSON files
all_raw_data = []

# Iterate over files in the current directory
for filename in os.listdir(current_directory):
    # Check if the filename matches the pattern 'gabonX.json' where X is between 1 and 40
    if filename.startswith("gabon") and filename.endswith(".json"):
        try:
            # Extract the number from the filename
            number_str = filename[len("gabon"):-len(".json")]
            file_number = int(number_str)
            if 1 <= file_number <= 40:
                filepath = os.path.join(current_directory, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        # If the JSON file contains a list of objects, extend the all_raw_data list
                        if isinstance(data, list):
                            all_raw_data.extend(data)
                        # If the JSON file contains a single object, append it to the list
                        elif isinstance(data, dict):
                            all_raw_data.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {filename}: {e}")
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
        except ValueError:
            # Ignore files that start with 'gabon' but don't have a valid number
            pass

# Dictionary to store consolidated data by name
consolidated_data = {}

for item in all_raw_data:
    title = item.get("Title")

    # Skip if title is missing
    if not title:
        continue

    # Initialize new item in consolidated_data if not already present
    if title not in consolidated_data:
        consolidated_data[title] = {
            "name": title,
            "opening_hours": [],
            "status": item.get("px2", "Unknown")  # Add status field
        }

    new_item = consolidated_data[title]

    # Update status if not already set (prioritize "Ouvert" over "Fermé" if mixed)
    if item.get("px2") == "Ouvert" or not new_item.get("status") or new_item["status"] == "Unknown":
        new_item["status"] = item.get("px2", "Unknown")

    # Image URL (take first non-empty, non-null value)
    if item.get("Image") and item["Image"].strip() not in ["null", ""] and not new_item.get("image_url"):
        new_item["image_url"] = item["Image"]

    # Category (take first non-empty, non-null value)
    if item.get("textsm") and item["textsm"].strip() not in ["null", ""] and not new_item.get("category"):
        new_item["category"] = item["textsm"]

    # City (take first non-empty, non-null value)
    if item.get("textsm2") and item["textsm2"].strip() not in ["null", ""] and not new_item.get("city"):
        new_item["city"] = item["textsm2"]

    # Phone Number (prioritize Field2_text, remove "tel:" prefix if present)
    phone_number = ""
    if item.get("Field2_text"):
        phone_number = item["Field2_text"].replace("tel:", "").strip()
    elif item.get("Field8_text"): # Fallback to Field8_text if Field2_text is not available
        phone_number = item["Field8_text"].replace("tel:", "").strip()
    if phone_number and phone_number not in ["null", ""] and not new_item.get("phone_number"):
        new_item["phone_number"] = phone_number

    # Email (from Text field, take first non-empty, non-null value)
    if item.get("Text") and item["Text"].strip() not in ["null", ""] and not new_item.get("email"):
        new_item["email"] = item["Text"].strip()

    # Description (prioritize Field11, then Field3, then textsm3)
    description_candidates = []
    if item.get("Field11") and item["Field11"].strip() not in ["null", ""]:
        description_candidates.append(item["Field11"].strip())
    if item.get("Field3") and item["Field3"].strip() not in ["null", ""]:
        description_candidates.append(item["Field3"].strip())
    if item.get("textsm3") and item["textsm3"].strip() not in ["null", ""]:
        description_candidates.append(item["textsm3"].strip())

    if description_candidates and not new_item.get("description"):
        # Find the longest description among the candidates
        longest_description = ""
        for desc in description_candidates:
            if len(desc) > len(longest_description):
                longest_description = desc
        new_item["description"] = longest_description

    # Address (from Field3, take first non-empty, non-null value)
    if item.get("Field3") and item["Field3"].strip() not in ["null", ""] and not new_item.get("address"):
        new_item["address"] = item["Field3"].strip()

    # Ratings (from Field14, take first non-empty, non-null value)
    if item.get("Field14") and item["Field14"].strip() not in ["null", ""] and not new_item.get("ratings"):
        new_item["ratings"] = item["Field14"].strip()

    # Opening Hours (from Field15)
    if item.get("Field15") and item["Field15"].strip() not in ["", "Horaires non disponibles", "null"]:
        opening_hours = item["Field15"].replace('\n', ' ').strip()
        if opening_hours not in new_item["opening_hours"]:
            new_item["opening_hours"].append(opening_hours)

# Convert consolidated_data to list and clean up opening_hours
processed_data = []
for item in consolidated_data.values():
    if item["opening_hours"]:
        item["opening_hours"] = "; ".join(item["opening_hours"])
    else:
        del item["opening_hours"]  # Remove empty opening_hours
    # Remove any empty or null fields
    item = {k: v for k, v in item.items() if v not in ["", "null", []]}
    if "name" in item:
        processed_data.append(item)

# Save the processed data to a new JSON file in the CWD
try:
    with open(output_file_name, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, indent=4, ensure_ascii=False)
    print(f"Processed data saved to {output_file_name} in the current working directory.")
except Exception as e:
    print(f"Error saving processed data to {output_file_name}: {e}")

print("Script execution completed.")