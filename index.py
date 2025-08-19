import os
import json
import logging
from datetime import datetime
from flask import Flask, request, jsonify, render_template
import requests

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key")

# Monday.com API configuration
MONDAY_API_TOKEN = os.environ.get("MONDAY_API_TOKEN", "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjQxMDM1MDMyNiwiYWFpIjoxMSwidWlkIjo1NTIyMDQ0LCJpYWQiOiIyMDI0LTA5LTEzVDExOjUyOjQzLjAwMFoiLCJwZXIiOiJtZTp3cml0ZSIsImFjdGlkIjozNzk1MywicmduIjoidXNlMSJ9.hwTlwMwtbhKdZsYcGT7UoENBLZUAxnfUXchj5RZJBz4")
MONDAY_API_URL = "https://api.monday.com/v2"

# In-memory storage for operation state (in production, use a database)
operation_state = {}

def make_monday_api_request(query, variables=None):
    """
    Make a request to Monday.com API
    In development, this returns mock data with detailed comments
    """
    headers = {
        "Authorization": f"Bearer {MONDAY_API_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "query": query,
        "variables": variables or {}
    }

    logger.debug(f"Monday API Request: {json.dumps(payload, indent=2)}")

    # Real API call
    response = None
    try:
        response = requests.post(MONDAY_API_URL, json=payload, headers=headers)
        logger.debug(f"API Response Status: {response.status_code}")
        response.raise_for_status()
        
        json_response = response.json()
        if json_response is None:
            logger.error("Monday API returned None JSON response")
            return None
        if not isinstance(json_response, dict):
            logger.error(f"Monday API returned non-dict response: {type(json_response)}")
            return None
        
        # Check for GraphQL errors
        if "errors" in json_response:
            logger.error(f"Monday API returned GraphQL errors: {json_response['errors']}")
            return None
            
        return json_response
    except requests.exceptions.RequestException as e:
        logger.error(f"Monday API request failed: {e}")
        if response:
            logger.error(f"Response content: {response.text}")
        return None
    except (ValueError, TypeError) as e:
        logger.error(f"Monday API JSON decode failed: {e}")
        if response:
            logger.error(f"Response content: {response.text}")
        return None

def get_subitems_by_group_and_name(group_id, item_name):
    """
    Retrieve subitems from a specific group where item name matches parent item name
    """
    # Use the specific board ID provided
    board_id = "9431708170"

    query = f"""
    query {{
        boards(ids: {board_id}) {{
            items_page(limit: 50, query_params: {{rules: [{{column_id: "group", compare_value: ["{group_id}"], operator: any_of}}]}}) {{
                items {{
                    id
                    name
                    group {{
                        id
                        title
                    }}
                    subitems {{
                        id
                        name
                        board {{
                            id
                        }}
                        column_values {{
                            id
                            value
                            text
                        }}
                    }}
                }}
            }}
        }}
    }}
    """

    response = make_monday_api_request(query)

    # Extract subitems from response
    subitems = []
    try:
        if not response or not isinstance(response, dict):
            logger.error(f"Invalid response from Monday API for get_subitems_by_group_and_name: {response}")
            return [] # Return empty list on invalid response

        for board in response.get("data", {}).get("boards", []):
            items_page = board.get("items_page", {})
            for item in items_page.get("items", []):
                if item.get("name") == item_name:
                    item_subitems = item.get("subitems", [])
                    logger.info(f"Found {len(item_subitems)} subitems for item {item_name}")
                    subitems.extend(item_subitems)

        # If no exact match found, log available items for debugging
        if not subitems:
            logger.info("Available items in the group:")
            for board in response.get("data", {}).get("boards", []):
                items_page = board.get("items_page", {})
                for item in items_page.get("items", []):
                    logger.info(f"  - {item.get('name')} (subitems: {len(item.get('subitems', []))})")

    except Exception as e:
        logger.error(f"Error parsing subitems response: {e}")

    return subitems

def get_item_data(item_id, item_name):
    """
    Get item data with required columns from Monday.com
    """
    board_id = "9431708170"

    query = f"""
    query {{
        items(ids: [{item_id}]) {{
            id
            name
            column_values(ids: ["color_mks7xywc", "numeric_mks61nvq", "numeric_mksxcdva"]) {{
                id
                value
            }}
        }}
    }}
    """

    response = make_monday_api_request(query)

    # Extract item data from response
    item_data = {
        "id": item_id,
        "name": item_name,
        "color_mks7xywc": "",
        "numeric_mks61nvq": 0,
        "numeric_mksxcdva": 0
    }

    try:
        if not response or not isinstance(response, dict):
            logger.error(f"Invalid response from Monday API for get_item_data: {response}")
            return item_data # Return default item_data on invalid response

        items = response.get("data", {}).get("items", [])
        if items:
            item = items[0]
            for col in item.get("column_values", []):
                if col["id"] == "color_mks7xywc":
                    item_data["color_mks7xywc"] = col["value"] or ""
                elif col["id"] == "numeric_mks61nvq":
                    try:
                        value_str = col["value"] or "0"
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        item_data["numeric_mks61nvq"] = float(value_str)
                    except (ValueError, TypeError):
                        item_data["numeric_mks61nvq"] = 0
                elif col["id"] == "numeric_mksxcdva":
                    try:
                        value_str = col["value"] or "0"
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        item_data["numeric_mksxcdva"] = float(value_str)
                    except (ValueError, TypeError):
                        item_data["numeric_mksxcdva"] = 0
    except Exception as e:
        logger.error(f"Error parsing item data response: {e}")

    return item_data

def update_subitem_column(subitem_id, column_id, value, subitem_board_id="9431861361"):
    """
    Update a specific column value for a subitem
    """
    # Subitems are updated using the standard change_column_value mutation with their board_id
    query = """
    mutation($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
        change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
            id
        }
    }
    """

    variables = {
        "boardId": str(subitem_board_id),
        "itemId": str(subitem_id),
        "columnId": column_id,
        "value": str(value)
    }

    try:
        response = make_monday_api_request(query, variables)
        if response is None:
            logger.error(f"update_subitem_column received None response for subitem {subitem_id}, column {column_id}")
            return None
        if not isinstance(response, dict):
            logger.error(f"update_subitem_column received non-dict response: {type(response)} for subitem {subitem_id}")
            return None
        return response
    except Exception as e:
        logger.error(f"update_subitem_column failed for subitem {subitem_id}, column {column_id}: {e}")
        import traceback
        logger.error(f"update_subitem_column traceback: {traceback.format_exc()}")
        return None

def duplicate_subitem(subitem_id, new_name):
    """
    Duplicate a subitem with a new name
    """
    query = """
    mutation($boardId: ID!, $itemId: ID!) {
        duplicate_item(board_id: $boardId, item_id: $itemId) {
            id
        }
    }
    """

    variables = {
        "boardId": "9431861361",  # Subitem board ID
        "itemId": str(subitem_id)
    }

    response = make_monday_api_request(query, variables)

    if not response or not isinstance(response, dict):
        logger.error(f"Invalid response from Monday API for duplicate_subitem: {response}")
        return None

    # Update the name of the duplicated item
    if response.get("data", {}).get("duplicate_item", {}).get("id"):
        new_item_id = response["data"]["duplicate_item"]["id"]

        # Update name using change_simple_column_value for name column
        update_query = """
        mutation($boardId: ID!, $itemId: ID!, $value: String!) {
            change_simple_column_value(board_id: $boardId, item_id: $itemId, column_id: "name", value: $value) {
                id
            }
        }
        """

        update_variables = {
            "boardId": "9431861361",  # Subitem board ID
            "itemId": str(new_item_id),
            "value": new_name
        }

        make_monday_api_request(update_query, update_variables)
        logger.info(f"Duplicated subitem {subitem_id} -> {new_item_id} with name '{new_name}'")
        return new_item_id

    return None

def delete_item(item_id, board_id="9431861361"):
    """
    Delete an item from Monday.com
    """
    query = """
    mutation($itemId: ID!) {
        delete_item(item_id: $itemId) {
            id
        }
    }
    """

    variables = {
        "itemId": str(item_id)
    }

    response = make_monday_api_request(query, variables)

    if not response or not isinstance(response, dict):
        logger.error(f"Invalid response from Monday API for delete_item: {response}")
        return False

    if response.get("data", {}).get("delete_item", {}).get("id"):
        logger.info(f"Successfully deleted item {item_id}")
        return True
    else:
        logger.error(f"Failed to delete item {item_id}")
        return False

def distribute_values(item_data, max_splits=None):
    """
    Main logic for distributing values across subitems
    Distributes numeric_mks61nvq value and saves numeric_mksxcdva to eligible subitems
    max_splits: if set, limits the number of subitems that can be split
    """
    try:
        # Extract data from payload
        limit_value = float(item_data.get("numeric_mks61nvq", 0))
        numeric_value_to_save = float(item_data.get("numeric_mksxcdva", 0))
        currency_dropdown = item_data.get("color_mks7xywc", "")
        item_name = item_data.get("name", "")
        item_id = item_data.get("id", "")

        logger.info(f"Processing item: {item_name} (ID: {item_id})")
        logger.info(f"Values - Limit to distribute: {limit_value}, Value to save: {numeric_value_to_save}, Currency: {currency_dropdown}")

        # Determine group based on currency - handle status column format
        group_id = "group_mks6z9xe"

        # Check if we have a valid currency status value
        valid_currency = False
        is_dollar = False

        # Status columns in Monday.com can have different formats
        if currency_dropdown:
            try:
                # Try to parse as JSON first (Monday.com status format)
                parsed_status = json.loads(currency_dropdown)
                if "index" in parsed_status:
                    # Status column with index - check for valid currency indices
                    status_index = parsed_status["index"]
                    if status_index in [0, 1, 2, 3, 4]:  # Accept various status indices for currencies
                        valid_currency = True
                        # Determine if it's dollar based on index (you may need to adjust these indices)
                        is_dollar = status_index in [1, 2]  # Adjust based on your status column setup
                        logger.info(f"Accepted currency status with index: {status_index}, is_dollar: {is_dollar}")
                elif "label" in parsed_status:
                    # Status column with label
                    label = parsed_status["label"].upper()
                    if "DÓLAR" in label or "DOLLAR" in label or "$" in label:
                        valid_currency = True
                        is_dollar = True
                    elif "EURO" in label or "€" in label:
                        valid_currency = True
                        is_dollar = False
                    logger.info(f"Accepted currency status with label: {parsed_status['label']}, is_dollar: {is_dollar}")
            except (json.JSONDecodeError, TypeError):
                # If it's not JSON, treat as plain text
                currency_text = currency_dropdown.upper()
                if "DÓLAR" in currency_text or "DOLLAR" in currency_text or "$" in currency_text:
                    valid_currency = True
                    is_dollar = True
                elif "EURO" in currency_text or "€" in currency_text:
                    valid_currency = True
                    is_dollar = False
                logger.info(f"Accepted currency text: {currency_dropdown}, is_dollar: {is_dollar}")

        if not valid_currency:
            logger.error(f"Invalid currency status value: {currency_dropdown}")
            return {"error": "Invalid currency status value"}, 400

        # Get subitems from the parent item with same name in group_mks6z9xe
        logger.info(f"Looking for parent item '{item_name}' in group '{group_id}' to get its subitems")
        subitems = get_subitems_by_group_and_name(group_id, item_name)

        if not subitems:
            logger.warning(f"No subitems found for parent item '{item_name}' in group '{group_id}'")
            return {"message": f"No subitems found for parent item '{item_name}' in group '{group_id}'"}, 200

        # Find eligible subitems with "Parte Terrestre Internacional" status and empty numeric_mks6p0bv
        valid_status_values = [
            "Parte Terrestre Internacional"
        ]

        eligible_subitems = []

        for i, subitem in enumerate(subitems):
            status_text = None
            numeric_mks6p0bv_value = 0

            for col in subitem.get("column_values", []):
                if col["id"] == "color_mks7xmpz":
                    # Handle status column format
                    status_value = col.get("value", "")
                    if status_value:
                        try:
                            # Try to parse JSON format first
                            parsed_status = json.loads(status_value)
                            if "label" in parsed_status:
                                status_text = parsed_status["label"]
                            else:
                                status_text = col.get("text", "")
                        except (json.JSONDecodeError, TypeError):
                            # If not JSON, use text value
                            status_text = col.get("text", "")
                elif col["id"] == "numeric_mks6p0bv":
                    try:
                        value_str = col["value"] or "0"
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        numeric_mks6p0bv_value = float(value_str)
                    except (ValueError, TypeError):
                        numeric_mks6p0bv_value = 0

            # Check if subitem has eligible tipo and is empty
            if status_text in valid_status_values and numeric_mks6p0bv_value == 0:
                eligible_subitems.append({
                    'subitem': subitem,
                    'index': i,
                    'status_text': status_text
                })
                logger.info(f"Found eligible subitem: {subitem.get('name')} (index {i}) with tipo: {status_text}")

        if not eligible_subitems:
            logger.warning("No eligible empty subitems found")
            return {"message": "No eligible empty subitems found"}, 200

        logger.info(f"Found {len(eligible_subitems)} eligible subitems to process")

        # Determine deduction column based on currency
        deduction_column = "numeric_mks6ywg8"  # Default to Euro column
        if is_dollar:
            deduction_column = "numeric_mks6myhs"

        logger.info(f"Using deduction column: {deduction_column} for currency (is_dollar: {is_dollar})")

        # Distribute the limit_value across eligible subitems
        remaining_value = limit_value
        processed_subitems = []
        last_eligible_numeric_mks6p0bv = numeric_value_to_save
        splits_created = 0

        for item in eligible_subitems:
            if remaining_value <= 0:
                break

            subitem = item['subitem']
            subitem_board_id = subitem.get("board", {}).get("id", "9431861361")
            
            # Get subitem deduction value from numeric_mktm79z5 column
            subitem_deduction_value = 0
            for col in subitem.get("column_values", []):
                if col["id"] == "numeric_mktm79z5":
                    try:
                        raw_value = col["value"] or "0"
                        clean_value = raw_value.strip('"') if isinstance(raw_value, str) else str(raw_value)
                        subitem_deduction_value = float(clean_value)
                    except (ValueError, TypeError):
                        subitem_deduction_value = 0
                    break

            # Process subitem if there's a deduction value in numeric_mktm79z5
            if subitem_deduction_value > 0:
                # Check condition: numeric_mktm79z5 > numeric_mktzwbep (subitem_deduction_value > remaining_value)
                if subitem_deduction_value > remaining_value:
                    # Check if we've reached the split limit
                    if max_splits is not None and splits_created >= max_splits:
                        logger.info(f"Reached max splits limit ({max_splits}), stopping distribution")
                        break
                    
                    # Split required: numeric_mktm79z5 > numeric_mktzwbep
                    logger.info(f"Subitem {subitem['name']} deduction {subitem_deduction_value} is greater than remaining {remaining_value} - splitting")

                    # Get original name and values for splitting
                    original_name = subitem['name']
                    original_numeric_mktm79z5 = subitem_deduction_value
                    
                    # STEP 1: Update ORIGINAL subitem: numeric_mktm79z5 = numeric_mktzwbep (remaining_value)
                    update_original_limit_response = update_subitem_column(subitem["id"], "numeric_mktm79z5", remaining_value, subitem_board_id)
                    
                    if update_original_limit_response and update_original_limit_response.get("data", {}).get("change_column_value", {}).get("id"):
                        logger.info(f"Updated ORIGINAL subitem {subitem['name']} numeric_mktm79z5 from {original_numeric_mktm79z5} to {remaining_value}")
                        
                        # STEP 2: Update ORIGINAL subitem: numeric_mks6p0bv = numeric_mktzrf7x (numeric_value_to_save)
                        update_original_numeric_response = update_subitem_column(subitem["id"], "numeric_mks6p0bv", numeric_value_to_save, subitem_board_id)
                        
                        if update_original_numeric_response and update_original_numeric_response.get("data", {}).get("change_column_value", {}).get("id"):
                            logger.info(f"Updated ORIGINAL subitem {subitem['name']} numeric_mks6p0bv to {numeric_value_to_save}")
                            
                            # STEP 3: Duplicate ORIGINAL subitem to create PARTE 2
                            part2_id = duplicate_subitem(subitem["id"], f"{original_name} - Parte 2")

                            if part2_id:
                                # STEP 4: Update PARTE 2 numeric_mktm79z5 = old_original_numeric_mktm79z5 - current_original_numeric_mktm79z5
                                part2_limit = original_numeric_mktm79z5 - remaining_value
                                update_response_part2_limit = update_subitem_column(part2_id, "numeric_mktm79z5", part2_limit, subitem_board_id)

                                if update_response_part2_limit and update_response_part2_limit.get("data", {}).get("change_column_value", {}).get("id"):
                                    logger.info(f"Set PARTE 2 numeric_mktm79z5 to {part2_limit} ({original_numeric_mktm79z5} - {remaining_value})")
                                    
                                    # STEP 5: Update PARTE 2 numeric_mks6p0bv = numeric_mktzrf7x (numeric_value_to_save)
                                    update_response_part2_numeric = update_subitem_column(part2_id, "numeric_mks6p0bv", numeric_value_to_save, subitem_board_id)
                                    
                                    if update_response_part2_numeric and update_response_part2_numeric.get("data", {}).get("change_column_value", {}).get("id"):
                                        logger.info(f"Updated PARTE 2 numeric_mks6p0bv to {numeric_value_to_save}")
                                        
                                        # STEP 6: Rename ORIGINAL → "Original Subitem - Parte 1"
                                        rename_query = """
                                        mutation($boardId: ID!, $itemId: ID!, $value: String!) {
                                            change_simple_column_value(board_id: $boardId, item_id: $itemId, column_id: "name", value: $value) {
                                                id
                                            }
                                        }
                                        """

                                        rename_variables = {
                                            "boardId": subitem_board_id,
                                            "itemId": str(subitem["id"]),
                                            "value": f"{original_name} - Parte 1"
                                        }

                                        rename_response = make_monday_api_request(rename_query, rename_variables)
                                        
                                        if rename_response and rename_response.get("data", {}).get("change_simple_column_value", {}).get("id"):
                                            logger.info(f"Renamed ORIGINAL subitem to {original_name} - Parte 1")
                                            
                                            # All remaining value is consumed
                                            remaining_value = 0

                                            processed_subitems.append({
                                                "id": subitem["id"],
                                                "name": f"{original_name} - Parte 1",
                                                "assigned_value": numeric_value_to_save,
                                                "deducted_value": remaining_value  # Was remaining_value before being set to 0
                                            })

                                            processed_subitems.append({
                                                "id": part2_id,
                                                "name": f"{original_name} - Parte 2",
                                                "assigned_value": numeric_value_to_save,
                                                "deducted_value": part2_limit
                                            })

                                            logger.info(f"Split {original_name}: Parte 1 and Parte 2 both processed")
                                            logger.info(f"Parte 1: numeric_mktm79z5={remaining_value}, numeric_mks6p0bv={numeric_value_to_save}")
                                            logger.info(f"Parte 2: numeric_mktm79z5={part2_limit}, numeric_mks6p0bv={numeric_value_to_save}")

                                            splits_created += 1
                                            break  # Stop processing after first split to limit to 2 subitems
                                        else:
                                            logger.error(f"Failed to rename ORIGINAL subitem {subitem['name']} to Parte 1")
                                            break
                                    else:
                                        logger.error(f"Failed to update PARTE 2 numeric_mks6p0bv for {subitem['name']}")
                                        break
                                else:
                                    logger.error(f"Failed to update PARTE 2 numeric_mktm79z5 for split subitem {subitem['name']}")
                                    break
                            else:
                                logger.error(f"Failed to duplicate subitem {subitem['name']}")
                                break
                        else:
                            logger.error(f"Failed to update ORIGINAL subitem {subitem['name']} numeric_mks6p0bv")
                            break
                    else:
                        logger.error(f"Failed to update ORIGINAL subitem {subitem['name']} numeric_mktm79z5")
                        break
                else:
                    # Normal processing - numeric_mktm79z5 <= numeric_mktzwbep, process like /distribuir
                    logger.info(f"Subitem {subitem['name']} deduction {subitem_deduction_value} is <= remaining {remaining_value} - normal processing")
                    
                    # Update subitem numeric_mks6p0bv with the value to save
                    update_response = update_subitem_column(subitem["id"], "numeric_mks6p0bv", numeric_value_to_save, subitem_board_id)
                    if update_response and update_response.get("data", {}).get("change_column_value", {}).get("id"):
                        # Deduct the subitem's deduction value from remaining
                        remaining_value -= subitem_deduction_value
                        
                        processed_subitems.append({
                            "id": subitem["id"],
                            "name": subitem["name"],
                            "assigned_value": numeric_value_to_save,
                            "deducted_value": subitem_deduction_value
                        })
                        logger.info(f"Processed subitem {subitem['name']}: saved {numeric_value_to_save} to numeric_mks6p0bv, deducted {subitem_deduction_value}, remaining: {remaining_value}")
                        
                        # Continue to next subitem if there's still remaining value
                        if remaining_value <= 0:
                            logger.info(f"Remaining value is {remaining_value}, stopping distribution")
                            break
                    else:
                        logger.error(f"Failed to update subitem {subitem['name']}, skipping")
                        continue
            else:
                logger.info(f"Skipped subitem {subitem.get('name')}: no value in numeric_mktm79z5")

        # If there's a remaining value and we processed at least one subitem, save it as RESERVA
        if remaining_value > 0 and processed_subitems:
            # Find the parent item in group_mks6z9xe with the same name to save RESERVA
            parent_board_id = "9431708170"
            
            # Query to find the parent item in group_mks6z9xe
            parent_query = f"""
            query {{
                boards(ids: {parent_board_id}) {{
                    items_page(limit: 50, query_params: {{rules: [{{column_id: "group", compare_value: ["group_mks6z9xe"], operator: any_of}}]}}) {{
                        items {{
                            id
                            name
                        }}
                    }}
                }}
            }}
            """
            
            parent_response = make_monday_api_request(parent_query)
            parent_item_to_update = None
            
            if parent_response and isinstance(parent_response, dict):
                for board in parent_response.get("data", {}).get("boards", []):
                    items_page = board.get("items_page", {})
                    for item in items_page.get("items", []):
                        if item.get("name") == item_name:
                            parent_item_to_update = item.get("id")
                            break
                    if parent_item_to_update:
                        break
            
            if parent_item_to_update:
                # Update parent item with RESERVA value (numeric_mktzwbep) and last eligible numeric_mks6p0bv (numeric_mktzrf7x)
                reserva_update_query = """
                mutation($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
                    change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
                        id
                    }
                }
                """
                
                # Update RESERVA value
                reserva_variables = {
                    "boardId": parent_board_id,
                    "itemId": str(parent_item_to_update),
                    "columnId": "numeric_mktzwbep",
                    "value": str(remaining_value)
                }
                
                reserva_response = make_monday_api_request(reserva_update_query, reserva_variables)
                
                # Update last eligible subitem's numeric_mks6p0bv value
                cotacao_variables = {
                    "boardId": parent_board_id,
                    "itemId": str(parent_item_to_update),
                    "columnId": "numeric_mktzrf7x",
                    "value": str(last_eligible_numeric_mks6p0bv)
                }
                
                cotacao_response = make_monday_api_request(reserva_update_query, cotacao_variables)
                
                if (reserva_response and reserva_response.get("data", {}).get("change_column_value", {}).get("id") and
                    cotacao_response and cotacao_response.get("data", {}).get("change_column_value", {}).get("id")):
                    logger.info(f"Saved RESERVA {remaining_value} and last eligible value {last_eligible_numeric_mks6p0bv} to parent item {item_name} (ID: {parent_item_to_update})")
                else:
                    logger.error(f"Failed to save RESERVA to parent item {item_name}")
            else:
                logger.error(f"Could not find parent item {item_name} in group_mks6z9xe to save RESERVA")

        # Store operation state
        operation_state[item_id] = {
            "timestamp": datetime.now().isoformat(),
            "processed_subitems": processed_subitems,
            "remaining_value": remaining_value,
            "reserva_saved": remaining_value > 0 and processed_subitems
        }

        return {
            "message": "Values distributed successfully",
            "processed_subitems": processed_subitems,
            "remaining_value": remaining_value,
            "reserva_saved": remaining_value > 0 and processed_subitems
        }, 200

    except Exception as e:
        logger.error(f"Error in distribute_values: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {"error": str(e)}, 500

@app.route('/')
def index():
    """Main page with webhook information"""
    return render_template('index.html')

@app.route('/status')
def status():
    """Status page showing recent operations"""
    return render_template('status.html', operations=operation_state)

@app.route('/test-api')
def test_api():
    """Test Monday.com API connection"""
    try:
        # Simple query to test API connection
        test_query = """
        query {
            me {
                id
                name
                email
            }
        }
        """

        response = make_monday_api_request(test_query)
        if not response or not isinstance(response, dict):
            logger.error(f"Invalid response from Monday API for test_api: {response}")
            return jsonify({"status": "error", "error": "Invalid API response"}), 500

        return jsonify({
            "status": "success",
            "response": response
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/explore-board')
def explore_board():
    """Explore board structure"""
    try:
        # Query to get items from a specific group - correct syntax
        explore_query = """
        query {
            boards(ids: 9431708170) {
                id
                name
                items_page(limit: 10, query_params: {rules: [{column_id: "group", compare_value: ["group_mks6z9xe"], operator: any_of}]}) {
                    items {
                        id
                        name
                        group {
                            id
                            title
                        }
                        subitems {
                            id
                            name
                            column_values {
                                id
                                value
                            }
                        }
                    }
                }
            }
        }
        """

        response = make_monday_api_request(explore_query)
        if not response or not isinstance(response, dict):
            logger.error(f"Invalid response from Monday API for explore_board: {response}")
            return jsonify({"status": "error", "error": "Invalid API response"}), 500

        return jsonify({
            "status": "success",
            "response": response
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/somarsaidas', methods=['POST'])
def somar_saidas():
    """
    Webhook endpoint for summing subitem values and updating parent item
    """
    try:
        # Validate request
        if not request.is_json:
            logger.error("Request is not JSON")
            return jsonify({"error": "Request must be JSON"}), 400

        payload = request.get_json()

        if not payload:
            logger.error("Empty payload received")
            return jsonify({"error": "Empty payload"}), 400

        # Handle Monday.com challenge verification
        if 'challenge' in payload:
            logger.info("Received Monday.com challenge verification")
            challenge = payload['challenge']
            return jsonify({'challenge': challenge})

        logger.info(f"Received webhook payload: {json.dumps(payload, indent=2)}")

        # Extract parentItemId from Monday.com webhook payload
        parent_item_id = None

        # Handle different Monday.com webhook formats
        if "event" in payload and "parentItemId" in payload["event"]:
            parent_item_id = payload["event"]["parentItemId"]
        elif "event" in payload and "data" in payload["event"]:
            event_data = payload["event"]["data"]
            parent_item_id = event_data.get("parentItemId", "")
        elif "parentItemId" in payload:
            parent_item_id = payload["parentItemId"]
        else:
            logger.error("Could not extract parentItemId from webhook payload")
            return jsonify({"error": "parentItemId field is required"}), 400

        if not parent_item_id:
            logger.error("parentItemId is empty")
            return jsonify({"error": "parentItemId cannot be empty"}), 400

        # Query for the item name using parentItemId
        item_query = f"""
        query {{
            items(ids: [{parent_item_id}]) {{
                id
                name
            }}
        }}
        """

        item_response = make_monday_api_request(item_query)

        if not item_response or not isinstance(item_response, dict):
            logger.error(f"Invalid response from Monday API for item query: {item_response}")
            return jsonify({"error": "Failed to get parent item data"}), 500

        # Extract the item name to use as num_negocio
        num_negocio = None
        try:
            items = item_response.get("data", {}).get("items", [])
            if items:
                item = items[0]
                num_negocio = item.get("name", "")
                logger.info(f"Found item with parentItemId {parent_item_id}: name = '{num_negocio}'")
            else:
                logger.error(f"No item found with ID {parent_item_id}")
                return jsonify({"error": f"Item with ID {parent_item_id} not found"}), 404
        except Exception as e:
            logger.error(f"Error extracting item name: {e}")
            return jsonify({"error": "Error extracting item name"}), 500

        if not num_negocio:
            logger.error("Item name is empty")
            return jsonify({"error": "Item name cannot be empty"}), 400

        print(f"num_negocio: {num_negocio}")
        logger.info(f"Processing num_negocio: {num_negocio}")

        # Query subitems only from the specific parent item (parentItemId)
        board_id = "9431708170"

        # Query to get ONLY the subitems from the specific parent item
        query_subitems = f"""
        query {{
            items(ids: [{parent_item_id}]) {{
                id
                name
                subitems {{
                    id
                    name
                    column_values(ids: ["text_mksxyrff", "formula_mks69snq"]) {{
                        id
                        value
                        text
                        ... on FormulaValue {{
                            value
                            id
                            display_value
                        }}
                    }}
                }}
            }}
        }}
        """

        response = make_monday_api_request(query_subitems)

        if not response or not isinstance(response, dict):
            logger.error(f"Invalid response from Monday API for subitems query: {response}")
            return jsonify({"error": "Failed to get subitems data"}), 500

        # Sum formula values from subitems of the specific parent item only
        parent_subitems = []
        total_sum = 0

        try:
            items = response.get("data", {}).get("items", [])
            if items:
                parent_item = items[0]
                item_subitems = parent_item.get("subitems", [])

                for subitem in item_subitems:
                    formula_value = 0

                    for col in subitem.get("column_values", []):
                        if col["id"] == "formula_mks69snq":
                            try:
                                # For formula columns, use display_value first, then value
                                raw_value = col.get("display_value") or col.get("value") or col.get("text") or "0"
                                if raw_value and raw_value != "null" and raw_value != "":
                                    # Remove quotes if present
                                    if isinstance(raw_value, str) and raw_value.startswith('"') and raw_value.endswith('"'):
                                        raw_value = raw_value[1:-1]
                                    formula_value = float(raw_value)
                                    print(f"  Found formula_mks69snq: {raw_value} -> {formula_value}")
                                else:
                                    formula_value = 0
                                    print(f"  Formula column empty or null")
                            except (ValueError, TypeError) as e:
                                formula_value = 0
                                print(f"  Error parsing formula_mks69snq: {e}")
                            break

                    # Add subitems from this specific parent item only
                    if formula_value > 0:
                        parent_subitems.append({
                            "id": subitem["id"],
                            "name": subitem["name"],
                            "formula_value": formula_value
                        })
                        total_sum += formula_value
                        print(f"✓ Subitem '{subitem['name']}' from parent '{parent_item['name']}' - formula_mks69snq: {formula_value}")
                        logger.info(f"Added subitem {subitem['name']} with value {formula_value} to sum")
            else:
                logger.error(f"Parent item with ID {parent_item_id} not found")
                return jsonify({"error": f"Parent item with ID {parent_item_id} not found"}), 404

        except Exception as e:
            logger.error(f"Error processing subitems: {e}")
            return jsonify({"error": "Error processing subitems"}), 500

        print(f"\n=== SUMA TOTAL ===")
        print(f"Parent Item: {num_negocio} (ID: {parent_item_id})")
        print(f"Total subitems procesados (from this parent only): {len(parent_subitems)}")
        print(f"Suma de formula_mks69snq: {total_sum}")
        print(f"================\n")

        logger.info(f"Found {len(parent_subitems)} subitems from parent item {parent_item_id} with total sum: {total_sum}")

        # Update the target item in board 9419091637 where item name equals num_negocio
        target_board_id = "9419091637"

        # Query to find the target item in the destination board
        query_target_item = f"""
        query {{
            boards(ids: {target_board_id}) {{
                items_page(limit: 100) {{
                    items {{
                        id
                        name
                        group {{
                            id
                        }}
                    }}
                }}
            }}
        }}
        """

        target_response = make_monday_api_request(query_target_item)

        if not target_response or not isinstance(target_response, dict):
            logger.error(f"Invalid response from Monday API for target query: {target_response}")
            return jsonify({"error": "Failed to get target board data"}), 500

        # Find the target item with name matching num_negocio
        target_item_id = None

        try:
            for board in target_response.get("data", {}).get("boards", []):
                items_page = board.get("items_page", {})
                for item in items_page.get("items", []):
                    if item.get("name") == num_negocio:
                        target_item_id = item.get("id")
                        logger.info(f"Found target item {item['name']} with ID {target_item_id}")
                        break
                if target_item_id:
                    break

        except Exception as e:
            logger.error(f"Error finding target item: {e}")
            return jsonify({"error": "Error finding target item"}), 500

        if not target_item_id:
            logger.error(f"Target item with name '{num_negocio}' not found in board {target_board_id}")
            return jsonify({"error": f"Target item '{num_negocio}' not found in destination board"}), 404

        # Update the numeric_mks6z16d column in the target item
        update_query = """
        mutation($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
            change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
                id
            }
        }
        """

        update_variables = {
            "boardId": target_board_id,
            "itemId": target_item_id,
            "columnId": "numeric_mks6z16d",
            "value": str(total_sum)
        }

        update_response = make_monday_api_request(update_query, update_variables)

        if not update_response or not isinstance(update_response, dict):
            logger.error(f"Invalid response from Monday API for update: {update_response}")
            return jsonify({"error": "Failed to update target item"}), 500

        if update_response.get("data", {}).get("change_column_value", {}).get("id"):
            logger.info(f"Successfully updated target item {num_negocio} with sum {total_sum}")

            return jsonify({
                "status": "ok",
                "soma": total_sum,
                "num_negocio": num_negocio,
                "parent_item_id": parent_item_id,
                "total_subitems_count": len(parent_subitems),
                "target_item_id": target_item_id
            }), 200
        else:
            logger.error(f"Failed to update target item {num_negocio}")
            return jsonify({"error": "Failed to update target item"}), 500

    except Exception as e:
        logger.error(f"Unexpected error in /somarsaidas endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/somarentradas', methods=['POST'])
def somar_entradas():
    """
    Webhook endpoint for summing subitem values and updating parent item
    Same logic as /somarsaidas but saves to numeric_mktjvpap column
    """
    try:
        # Validate request
        if not request.is_json:
            logger.error("Request is not JSON")
            return jsonify({"error": "Request must be JSON"}), 400

        payload = request.get_json()

        if not payload:
            logger.error("Empty payload received")
            return jsonify({"error": "Empty payload"}), 400

        # Handle Monday.com challenge verification
        if 'challenge' in payload:
            logger.info("Received Monday.com challenge verification")
            challenge = payload['challenge']
            return jsonify({'challenge': challenge})

        logger.info(f"Received webhook payload: {json.dumps(payload, indent=2)}")

        # Extract parentItemId from Monday.com webhook payload
        parent_item_id = None

        # Handle different Monday.com webhook formats
        if "event" in payload and "parentItemId" in payload["event"]:
            parent_item_id = payload["event"]["parentItemId"]
        elif "event" in payload and "data" in payload["event"]:
            event_data = payload["event"]["data"]
            parent_item_id = event_data.get("parentItemId", "")
        elif "parentItemId" in payload:
            parent_item_id = payload["parentItemId"]
        else:
            logger.error("Could not extract parentItemId from webhook payload")
            return jsonify({"error": "parentItemId field is required"}), 400

        if not parent_item_id:
            logger.error("parentItemId is empty")
            return jsonify({"error": "parentItemId cannot be empty"}), 400

        # Query for the item name using parentItemId
        item_query = f"""
        query {{
            items(ids: [{parent_item_id}]) {{
                id
                name
            }}
        }}
        """

        item_response = make_monday_api_request(item_query)

        if not item_response or not isinstance(item_response, dict):
            logger.error(f"Invalid response from Monday API for item query: {item_response}")
            return jsonify({"error": "Failed to get parent item data"}), 500

        # Extract the item name to use as num_negocio
        num_negocio = None
        try:
            items = item_response.get("data", {}).get("items", [])
            if items:
                item = items[0]
                num_negocio = item.get("name", "")
                logger.info(f"Found item with parentItemId {parent_item_id}: name = '{num_negocio}'")
            else:
                logger.error(f"No item found with ID {parent_item_id}")
                return jsonify({"error": f"Item with ID {parent_item_id} not found"}), 404
        except Exception as e:
            logger.error(f"Error extracting item name: {e}")
            return jsonify({"error": "Error extracting item name"}), 500

        if not num_negocio:
            logger.error("Item name is empty")
            return jsonify({"error": "Item name cannot be empty"}), 400

        print(f"num_negocio: {num_negocio}")
        logger.info(f"Processing num_negocio: {num_negocio}")

        # Query subitems only from the specific parent item (parentItemId)
        board_id = "9431708170"

        # Query to get ONLY the subitems from the specific parent item
        query_subitems = f"""
        query {{
            items(ids: [{parent_item_id}]) {{
                id
                name
                subitems {{
                    id
                    name
                    column_values(ids: ["text_mksxyrff", "formula_mks69snq"]) {{
                        id
                        value
                        text
                        ... on FormulaValue {{
                            value
                            id
                            display_value
                        }}
                    }}
                }}
            }}
        }}
        """

        response = make_monday_api_request(query_subitems)

        if not response or not isinstance(response, dict):
            logger.error(f"Invalid response from Monday API for subitems query: {response}")
            return jsonify({"error": "Failed to get subitems data"}), 500

        # Sum formula values from subitems of the specific parent item only
        parent_subitems = []
        total_sum = 0

        try:
            items = response.get("data", {}).get("items", [])
            if items:
                parent_item = items[0]
                item_subitems = parent_item.get("subitems", [])

                for subitem in item_subitems:
                    formula_value = 0

                    for col in subitem.get("column_values", []):
                        if col["id"] == "formula_mks69snq":
                            try:
                                # For formula columns, use display_value first, then value
                                raw_value = col.get("display_value") or col.get("value") or col.get("text") or "0"
                                if raw_value and raw_value != "null" and raw_value != "":
                                    # Remove quotes if present
                                    if isinstance(raw_value, str) and raw_value.startswith('"') and raw_value.endswith('"'):
                                        raw_value = raw_value[1:-1]
                                    formula_value = float(raw_value)
                                    print(f"  Found formula_mks69snq: {raw_value} -> {formula_value}")
                                else:
                                    formula_value = 0
                                    print(f"  Formula column empty or null")
                            except (ValueError, TypeError) as e:
                                formula_value = 0
                                print(f"  Error parsing formula_mks69snq: {e}")
                            break

                    # Add subitems from this specific parent item only
                    if formula_value > 0:
                        parent_subitems.append({
                            "id": subitem["id"],
                            "name": subitem["name"],
                            "formula_value": formula_value
                        })
                        total_sum += formula_value
                        print(f"✓ Subitem '{subitem['name']}' from parent '{parent_item['name']}' - formula_mks69snq: {formula_value}")
                        logger.info(f"Added subitem {subitem['name']} with value {formula_value} to sum")
            else:
                logger.error(f"Parent item with ID {parent_item_id} not found")
                return jsonify({"error": f"Parent item with ID {parent_item_id} not found"}), 404

        except Exception as e:
            logger.error(f"Error processing subitems: {e}")
            return jsonify({"error": "Error processing subitems"}), 500

        print(f"\n=== SUMA TOTAL ENTRADAS ===")
        print(f"Parent Item: {num_negocio} (ID: {parent_item_id})")
        print(f"Total subitems procesados (from this parent only): {len(parent_subitems)}")
        print(f"Suma de formula_mks69snq: {total_sum}")
        print(f"================\n")

        logger.info(f"Found {len(parent_subitems)} subitems from parent item {parent_item_id} with total sum: {total_sum}")

        # Update the target item in board 9419091637 where item name equals num_negocio
        target_board_id = "9419091637"

        # Query to find the target item in the destination board
        query_target_item = f"""
        query {{
            boards(ids: {target_board_id}) {{
                items_page(limit: 100) {{
                    items {{
                        id
                        name
                        group {{
                            id
                        }}
                    }}
                }}
            }}
        }}
        """

        target_response = make_monday_api_request(query_target_item)

        if not target_response or not isinstance(target_response, dict):
            logger.error(f"Invalid response from Monday API for target query: {target_response}")
            return jsonify({"error": "Failed to get target board data"}), 500

        # Find the target item with name matching num_negocio
        target_item_id = None

        try:
            for board in target_response.get("data", {}).get("boards", []):
                items_page = board.get("items_page", {})
                for item in items_page.get("items", []):
                    if item.get("name") == num_negocio:
                        target_item_id = item.get("id")
                        logger.info(f"Found target item {item['name']} with ID {target_item_id}")
                        break
                if target_item_id:
                    break

        except Exception as e:
            logger.error(f"Error finding target item: {e}")
            return jsonify({"error": "Error finding target item"}), 500

        if not target_item_id:
            logger.error(f"Target item with name '{num_negocio}' not found in board {target_board_id}")
            return jsonify({"error": f"Target item '{num_negocio}' not found in destination board"}), 404

        # Update the numeric_mktjvpap column in the target item (different from /somarsaidas)
        update_query = """
        mutation($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
            change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
                id
            }
        }
        """

        update_variables = {
            "boardId": target_board_id,
            "itemId": target_item_id,
            "columnId": "numeric_mktjvpap",  # Different column for entradas
            "value": str(total_sum)
        }

        update_response = make_monday_api_request(update_query, update_variables)

        if not update_response or not isinstance(update_response, dict):
            logger.error(f"Invalid response from Monday API for update: {update_response}")
            return jsonify({"error": "Failed to update target item"}), 500

        if update_response.get("data", {}).get("change_column_value", {}).get("id"):
            logger.info(f"Successfully updated target item {num_negocio} with entradas sum {total_sum}")

            return jsonify({
                "status": "ok",
                "soma": total_sum,
                "num_negocio": num_negocio,
                "parent_item_id": parent_item_id,
                "total_subitems_count": len(parent_subitems),
                "target_item_id": target_item_id,
                "column_updated": "numeric_mktjvpap"
            }), 200
        else:
            logger.error(f"Failed to update target item {num_negocio}")
            return jsonify({"error": "Failed to update target item"}), 500

    except Exception as e:
        logger.error(f"Unexpected error in /somarentradas endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/atualizarreservadecambio', methods=['POST'])
def atualizar_reserva_de_cambio():
    """
    Webhook endpoint for using RESERVA values for distribution
    Checks for RESERVA values in parent item and uses them for distribution like /distribuir
    """
    try:
        # Validate request
        if not request.is_json:
            logger.error("Request is not JSON")
            return jsonify({"error": "Request must be JSON"}), 400

        payload = request.get_json()

        if not payload:
            logger.error("Empty payload received")
            return jsonify({"error": "Empty payload"}), 400

        # Handle Monday.com challenge verification
        if 'challenge' in payload:
            logger.info("Received Monday.com challenge verification")
            challenge = payload['challenge']
            return jsonify({'challenge': challenge})

        logger.info(f"Received webhook payload: {json.dumps(payload, indent=2)}")

        # Extract parentItemId from Monday.com webhook payload
        parent_item_id = None

        # Handle Monday.com webhook format
        if "event" in payload and "parentItemId" in payload["event"]:
            parent_item_id = payload["event"]["parentItemId"]
        elif "event" in payload and "data" in payload["event"]:
            # Current webhook format with nested data
            event_data = payload["event"]["data"]
            parent_item_id = event_data.get("parent_item", "")
        elif "parent_item" in payload:
            parent_item_id = payload["parent_item"].get("id", "")
        else:
            logger.error("Could not extract parentItemId from webhook payload")
            return jsonify({"error": "parentItemId field is required"}), 400

        if not parent_item_id:
            logger.error("parentItemId is empty")
            return jsonify({"error": "parentItemId cannot be empty"}), 400

        # Query for the parent item name using parentItemId
        item_query = f"""
        query {{
            items(ids: [{parent_item_id}]) {{
                id
                name
            }}
        }}
        """

        item_response = make_monday_api_request(item_query)

        if not item_response or not isinstance(item_response, dict):
            logger.error(f"Invalid response from Monday API for item query: {item_response}")
            return jsonify({"error": "Failed to get parent item data"}), 500

        # Extract the parent item name
        item_name = None
        try:
            items = item_response.get("data", {}).get("items", [])
            if items:
                item = items[0]
                item_name = item.get("name", "")
                logger.info(f"Found parent item with parentItemId {parent_item_id}: name = '{item_name}'")
            else:
                logger.error(f"No parent item found with ID {parent_item_id}")
                return jsonify({"error": f"Parent item with ID {parent_item_id} not found"}), 404
        except Exception as e:
            logger.error(f"Error extracting parent item name: {e}")
            return jsonify({"error": "Error extracting parent item name"}), 500

        if not item_name:
            logger.error("Parent item name is empty")
            return jsonify({"error": "Parent item name cannot be empty"}), 400

        # Check for RESERVA values in the parent item in group_mks6z9xe
        group_id = "group_mks6z9xe"
        parent_board_id = "9431708170"
        
        # Query to find the parent item in group_mks6z9xe and get its RESERVA values plus currency status
        parent_query = f"""
        query {{
            boards(ids: {parent_board_id}) {{
                items_page(limit: 50, query_params: {{rules: [{{column_id: "group", compare_value: ["{group_id}"], operator: any_of}}]}}) {{
                    items {{
                        id
                        name
                        column_values(ids: ["numeric_mktzwbep", "numeric_mktzrf7x", "color_mks7xywc"]) {{
                            id
                            value
                        }}
                    }}
                }}
            }}
        }}
        """
        
        parent_response = make_monday_api_request(parent_query)
        
        reserva_found = False
        reserva_value = 0
        cotacao_value = 0
        currency_status = ""
        parent_item_with_reserva = None
        
        if parent_response and isinstance(parent_response, dict):
            for board in parent_response.get("data", {}).get("boards", []):
                items_page = board.get("items_page", {})
                for item in items_page.get("items", []):
                    if item.get("name") == item_name:
                        # Check for RESERVA, cotacao and currency values
                        current_reserva = 0
                        current_cotacao = 0
                        current_currency = ""
                        
                        for col in item.get("column_values", []):
                            if col["id"] == "numeric_mktzwbep":  # RESERVA column
                                try:
                                    value_str = col["value"] or "0"
                                    if value_str.startswith('"') and value_str.endswith('"'):
                                        value_str = value_str[1:-1]
                                    current_reserva = float(value_str)
                                except (ValueError, TypeError):
                                    current_reserva = 0
                            elif col["id"] == "numeric_mktzrf7x":  # Cotacao column
                                try:
                                    value_str = col["value"] or "0"
                                    if value_str.startswith('"') and value_str.endswith('"'):
                                        value_str = value_str[1:-1]
                                    current_cotacao = float(value_str)
                                except (ValueError, TypeError):
                                    current_cotacao = 0
                            elif col["id"] == "color_mks7xywc":  # Currency status column
                                current_currency = col["value"] or ""
                        
                        if current_reserva > 0:
                            reserva_found = True
                            reserva_value = current_reserva
                            cotacao_value = current_cotacao
                            currency_status = current_currency
                            parent_item_with_reserva = item.get("id")
                            logger.info(f"Found RESERVA {reserva_value} in parent item {item_name} with cotacao {cotacao_value}")
                            break
                if reserva_found:
                    break

        if not reserva_found:
            logger.info("No RESERVA values found in parent item - nothing to do")
            return jsonify({"message": "No RESERVA values found in parent item"}), 200

        # Create item_data structure similar to what distribute_values expects
        # Use the RESERVA value as the limit to distribute (numeric_mks61nvq)
        # Use the cotacao value as the value to save to eligible subitems (numeric_mksxcdva)
        item_data = {
            "id": parent_item_id,
            "name": item_name,
            "numeric_mks61nvq": reserva_value,  # Use RESERVA as limit
            "numeric_mksxcdva": cotacao_value,  # Use cotacao as value to save
            "color_mks7xywc": currency_status   # Use parent's currency status
        }

        logger.info(f"Using RESERVA for distribution - Limit: {reserva_value}, Value to save: {cotacao_value}")

        # Validate that we have the required data for distribution (status column must have a value)
        if not currency_status:
            logger.info(f"Parent item {item_name} has no status value set, skipping distribution")
            return jsonify({"message": "No status value set for distribution"}), 200

        if reserva_value <= 0:
            logger.info(f"Parent item {item_name} has no RESERVA value to distribute")
            return jsonify({"message": "No RESERVA value to distribute"}), 200

        # Process the distribution using the same logic as /distribuir
        # This will handle splitting, saving new RESERVA values, etc.
        # Limit to 1 split maximum (creates only 2 parts)
        try:
            distribution_result = distribute_values(item_data, max_splits=1)

            # Handle the case where distribute_values returns None or invalid data
            if distribution_result is None:
                logger.error("distribute_values returned None")
                return jsonify({
                    "error": "Distribution function returned None",
                    "reserva_used": reserva_value,
                    "cotacao_used": cotacao_value
                }), 500

            # distribute_values returns a tuple (result, status_code)
            if not isinstance(distribution_result, tuple) or len(distribution_result) != 2:
                logger.error(f"distribute_values returned invalid format: {type(distribution_result)}")
                return jsonify({
                    "error": "Distribution function returned invalid format",
                    "reserva_used": reserva_value,
                    "cotacao_used": cotacao_value
                }), 500

            result, status_code = distribution_result

            # Validate the result part
            if result is None:
                logger.error("distribute_values returned None as result")
                return jsonify({
                    "error": "Distribution function returned None result",
                    "reserva_used": reserva_value,
                    "cotacao_used": cotacao_value
                }), 500

        except Exception as e:
            logger.error(f"Error in distribute_values: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({
                "error": f"Distribution failed: {str(e)}",
                "reserva_used": reserva_value,
                "cotacao_used": cotacao_value
            }), 500

        # Only clear original RESERVA values if there was splitting (no remaining value)
        # When there's normal processing with remainder, keep the RESERVA values 
        should_clear_reserva = True
        if isinstance(result, dict) and result.get("remaining_value", 0) > 0:
            # If there's a remaining value, it means normal processing occurred and RESERVA should be preserved
            should_clear_reserva = False
            logger.info(f"Preserving RESERVA values as remaining value {result.get('remaining_value')} > 0")
        
        if should_clear_reserva:
            try:
                logger.info(f"Clearing original RESERVA values from parent item {parent_item_with_reserva}")
                
                # Clear original RESERVA value (numeric_mktzwbep)
                clear_reserva_query = """
                mutation($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
                    change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
                        id
                    }
                }
                """
                
                # Clear original RESERVA (set to 0)
                clear_reserva_variables = {
                    "boardId": parent_board_id,
                    "itemId": str(parent_item_with_reserva),
                    "columnId": "numeric_mktzwbep",
                    "value": "0"
                }
                
                clear_reserva_response = make_monday_api_request(clear_reserva_query, clear_reserva_variables)
                
                # Clear original cotacao (set to 0)
                clear_cotacao_variables = {
                    "boardId": parent_board_id,
                    "itemId": str(parent_item_with_reserva),
                    "columnId": "numeric_mktzrf7x", 
                    "value": "0"
                }
                
                clear_cotacao_response = make_monday_api_request(clear_reserva_query, clear_cotacao_variables)
                
                if (clear_reserva_response and clear_reserva_response.get("data", {}).get("change_column_value", {}).get("id") and
                    clear_cotacao_response and clear_cotacao_response.get("data", {}).get("change_column_value", {}).get("id")):
                    logger.info(f"Successfully cleared original RESERVA and cotacao values from parent item {parent_item_with_reserva}")
                else:
                    logger.error(f"Failed to clear original RESERVA values from parent item {parent_item_with_reserva}")
            except Exception as e:
                logger.error(f"Failed to clear original RESERVA values: {e}")
                import traceback
                logger.error(f"Clear RESERVA traceback: {traceback.format_exc()}")
                # Continue anyway, as the main distribution was successful
        else:
            logger.info(f"Keeping original RESERVA values as remainder exists: numeric_mktzwbep and numeric_mktzrf7x preserved")

        # Add RESERVA information to the result
        if isinstance(result, dict):
            result["used_reserva"] = {
                "source_parent_item_id": parent_item_with_reserva,
                "original_reserva_value": reserva_value,
                "original_cotacao_value": cotacao_value,
                "original_reserva_cleared": True
            }
        else:
            logger.error(f"Expected dict result from distribute_values, got: {type(result)}")
            return jsonify({
                "error": "Invalid result from distribution",
                "result_type": str(type(result)),
                "reserva_used": reserva_value,
                "cotacao_used": cotacao_value
            }), 500

        return jsonify(result), status_code

    except Exception as e:
        logger.error(f"Unexpected error in /atualizarreservadecambio endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/distribuir', methods=['POST'])
def distribuir():
    """
    Main webhook endpoint for Monday.com payloads
    Distributes numeric_mks61nvq value and saves numeric_mksxcdva to eligible subitems
    """
    try:
        # Validate request
        if not request.is_json:
            logger.error("Request is not JSON")
            return jsonify({"error": "Request must be JSON"}), 400

        payload = request.get_json()

        if not payload:
            logger.error("Empty payload received")
            return jsonify({"error": "Empty payload"}), 400

        # Handle Monday.com challenge verification
        if 'challenge' in payload:
            logger.info("Received Monday.com challenge verification")
            challenge = payload['challenge']
            return jsonify({'challenge': challenge})

        logger.info(f"Received webhook payload: {json.dumps(payload, indent=2)}")

        # Extract basic info from Monday.com webhook payload
        item_id = None
        item_name = None

        # Handle Monday.com webhook format
        if "event" in payload and "pulseId" in payload["event"]:
            item_id = payload["event"]["pulseId"]
            item_name = payload["event"].get("pulseName", "")
        elif "event" in payload and "data" in payload["event"]:
            # Current webhook format with nested data
            event_data = payload["event"]["data"]
            item_id = event_data.get("item_id", "")
            item_name = event_data.get("item_name", "")
        elif "item" in payload:
            # Alternative format or testing
            item_id = payload["item"].get("id", "")
            item_name = payload["item"].get("name", "")
        else:
            # Direct format for testing
            item_id = payload.get("id", "")
            item_name = payload.get("name", "")

        if not item_id or not item_name:
            logger.error("Could not extract item ID or name from webhook payload")
            return jsonify({"error": "Invalid webhook payload"}), 400

        # Query Monday.com to get the actual item data with required columns
        item_data = get_item_data(item_id, item_name)

        if not item_data or not isinstance(item_data, dict):
            logger.error("Could not retrieve item data or data is not a dictionary")
            return jsonify({"error": "Could not retrieve item data"}), 400

        # Validate that we have the required data (status column must have a value)
        if not item_data.get("color_mks7xywc"):
            logger.info(f"Item {item_name} has no status value set, skipping processing")
            return jsonify({"message": "No status value set, skipping processing"}), 200

        # Check if we have values to distribute
        if item_data.get("numeric_mks61nvq", 0) <= 0:
            logger.info(f"Item {item_name} has no limit value (numeric_mks61nvq) to distribute")
            return jsonify({"message": "No limit value to distribute"}), 200

        # Process the distribution
        result, status_code = distribute_values(item_data)

        return jsonify(result), status_code

    except Exception as e:
        logger.error(f"Unexpected error in webhook endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    # For local development
    app.run(host='0.0.0.0', port=5000, debug=True)