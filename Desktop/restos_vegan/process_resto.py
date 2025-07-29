import json
import os

def clean_and_rename_restaurant_data(data):
    """
    Cleans and renames fields in a list of restaurant data dictionaries.

    Args:
        data (list): A list of dictionaries, where each dictionary represents
                     a restaurant's information.

    Returns:
        list: A new list of dictionaries with cleaned and renamed fields.
    """
    cleaned_data = []
    for item in data:
        features_list = []

        if item.get("Field7"):
            features_list.append(item["Field7"])
        if item.get("Field8"):
            features_list.append(item["Field8"])
        if item.get("Field9") and "FriendVegan" in item["Field9"]:
             features_list.append("Vegan-friendly")

        new_item = {
            "name": item.get("Title"),
            "url": item.get("Title_URL"),
            "image_url": item.get("Image"),
            "rating": item.get("mr1"),
            "review_count": item.get("flex").replace("(", "").replace(")", "") if item.get("flex") else None,
            "cuisine_type": item.get("lineclamp1"),
            "status": item.get("flex1"),
            "abstract": item.get("Abstract"),
            "full_description": item.get("textgray800"),
            "phone_number": item.get("Number").replace("tel:", "") if item.get("Number") else None,
            "address": item.get("fontnormal"),
            "website": item.get("Field2"),
            "delivery_link": item.get("Field6_links") if item.get("Field6_text") == "Delivery" else None,
            "features": features_list,
            "thumbnail_images": [
                img for img in [item.get("Field11"), item.get("Field12"), item.get("Field13")] if img
            ]
        }
        cleaned_data.append(new_item)
    return cleaned_data

def main():
    input_file_name = "vegan_resto.json"
    output_file_name = "cleaned_vegan_resto.json"

    if not os.path.exists(input_file_name):
        print(f"Error: Input file '{input_file_name}' not found in the current directory.")
        print("Please make sure 'vegan_resto.json' is in the same folder as this script.")
        return

    try:
        with open(input_file_name, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{input_file_name}'. Please ensure it's a valid JSON file.")
        return
    except Exception as e:
        print(f"An unexpected error occurred while reading '{input_file_name}': {e}")
        return

    if not isinstance(raw_data, list):
        print(f"Warning: The root of '{input_file_name}' is not a list. The script expects a JSON array of restaurant objects.")
        if isinstance(raw_data, dict):
            print("Attempting to process it as a single restaurant object within a list.")
            raw_data = [raw_data]
        else:
            print("Error: The script cannot process the input JSON format. It must be a list of objects or a single dictionary object.")
            return

    processed_data = clean_and_rename_restaurant_data(raw_data)

    try:
        with open(output_file_name, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=4, ensure_ascii=False)
        print(f"Successfully processed data from '{input_file_name}' and saved to '{output_file_name}' in the current directory.")
    except Exception as e:
        print(f"An error occurred while writing the output file '{output_file_name}': {e}")

if __name__ == "__main__":
    main()