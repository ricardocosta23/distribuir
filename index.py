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
            column_values(ids: ["numeric_mks63qc1", "numeric_mks64nh2", "color_mks7xywc", "numeric_mks61nvq"]) {{
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
        "numeric_mks63qc1": 0,
        "numeric_mks64nh2": 0,
        "color_mks7xywc": "",
        "numeric_mks61nvq": 0
    }

    try:
        if not response or not isinstance(response, dict):
            logger.error(f"Invalid response from Monday API for get_item_data: {response}")
            return item_data # Return default item_data on invalid response

        items = response.get("data", {}).get("items", [])
        if items:
            item = items[0]
            for col in item.get("column_values", []):
                if col["id"] == "numeric_mks63qc1":
                    try:
                        # Monday.com returns numeric values as quoted strings
                        value_str = col["value"] or "0"
                        # Remove quotes if present
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        item_data["numeric_mks63qc1"] = float(value_str)
                    except (ValueError, TypeError):
                        item_data["numeric_mks63qc1"] = 0
                elif col["id"] == "numeric_mks64nh2":
                    try:
                        value_str = col["value"] or "0"
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        item_data["numeric_mks64nh2"] = float(value_str)
                    except (ValueError, TypeError):
                        item_data["numeric_mks64nh2"] = 0
                elif col["id"] == "color_mks7xywc":
                    item_data["color_mks7xywc"] = col["value"] or ""
                elif col["id"] == "numeric_mks61nvq":
                    try:
                        value_str = col["value"] or "0"
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        item_data["numeric_mks61nvq"] = float(value_str)
                    except (ValueError, TypeError):
                        item_data["numeric_mks61nvq"] = 0
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

def distribute_values(item_data):
    """
    Main logic for distributing values across subitems
    """
    try:
        # Extract data from payload
        numeric_value = float(item_data.get("numeric_mks63qc1", 0))
        formula_value = float(item_data.get("numeric_mks64nh2", 0))
        currency_dropdown = item_data.get("color_mks7xywc", "")
        limit_value = float(item_data.get("numeric_mks61nvq", 0))
        item_name = item_data.get("name", "")
        item_id = item_data.get("id", "")

        logger.info(f"Processing item: {item_name} (ID: {item_id})")
        logger.info(f"Values - Numeric: {numeric_value}, Formula: {formula_value}, Currency: {currency_dropdown}, Limit: {limit_value}")

        # Determine group based on currency - handle status column format
        group_id = "group_mks6z9xe"  # Both currencies use the same group

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

        # Debug: Log detailed information about the first few subitems to understand the data structure
        logger.debug(f"Total subitems found: {len(subitems)}")
        for i, subitem in enumerate(subitems[:3]):  # Log first 3 subitems
            logger.debug(f"Subitem {i+1} ({subitem.get('name')}): {json.dumps(subitem, indent=2)}")

        # Find the next eligible subitem with empty numeric_mks6p0bv to start processing
        # Valid status values for eligible "tipo" values
        valid_status_values = [
            "Parte Terrestre Internacional"
        ]

        # Find the starting point - first subitem with empty numeric_mks6p0bv and eligible tipo
        start_index = None
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
            if status_text in valid_status_values:
                eligible_subitems.append({
                    'subitem': subitem,
                    'index': i,
                    'is_empty': numeric_mks6p0bv_value == 0,
                    'status_text': status_text
                })

                # Mark the first empty eligible subitem as starting point
                if numeric_mks6p0bv_value == 0 and start_index is None:
                    start_index = i
                    logger.info(f"Found starting point at subitem {subitem.get('name')} (index {i}) with tipo: {status_text}")

        if not eligible_subitems:
            logger.warning("No subitems found with eligible tipo status values")
            return {"message": "No subitems found with eligible tipo status values"}, 200

        # Log all eligible subitems found for debugging
        logger.info(f"Found {len(eligible_subitems)} eligible subitems:")
        for item in eligible_subitems:
            status = "empty" if item['is_empty'] else "processed"
            logger.info(f"  - '{item['subitem']['name']}' (index {item['index']}): {status} - tipo: {item['status_text']}")

        if start_index is None:
            logger.info("All eligible subitems already processed")

            # Log detailed status for debugging
            status_summary = {}
            for item in eligible_subitems:
                status = "processed" if not item['is_empty'] else "empty"
                if status not in status_summary:
                    status_summary[status] = 0
                status_summary[status] += 1
                logger.info(f"Subitem '{item['subitem']['name']}' (index {item['index']}): {status} - tipo: {item['status_text']}")

            logger.info(f"Eligible subitems status: {status_summary}")
            return {
                "message": "All eligible subitems already processed",
                "status_summary": status_summary,
                "total_eligible": len(eligible_subitems)
            }, 200

        # Get all empty eligible subitems to process
        unprocessed_subitems = [item['subitem'] for item in eligible_subitems if item['is_empty']]

        logger.info(f"Starting distribution from index {start_index}, found {len(unprocessed_subitems)} empty eligible subitems to process")

        # Check for leftover sum from previous webhook execution
        leftover_sum = 0
        leftover_subitem_id = None
        leftover_numeric_value = 0  # Store the original numeric_mks6p0bv value to reuse

        # Look for the last eligible subitem with a value in numeric_mksxv8ep
        for item in reversed(eligible_subitems):  # Start from the last
            subitem = item['subitem']
            found_leftover = False

            # First check if this subitem has a leftover sum
            for col in subitem.get("column_values", []):
                if col["id"] == "numeric_mksxv8ep":
                    try:
                        value_str = col["value"] or "0"
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        potential_leftover = float(value_str)
                        if potential_leftover > 0:
                            leftover_sum = potential_leftover
                            leftover_subitem_id = subitem["id"]
                            found_leftover = True
                            logger.info(f"Found leftover sum {leftover_sum} in subitem {subitem['name']} (ID: {leftover_subitem_id})")
                            break
                    except (ValueError, TypeError):
                        continue

            # If we found a leftover, get the numeric_mks6p0bv value from the same subitem
            if found_leftover:
                for col in subitem.get("column_values", []):
                    if col["id"] == "numeric_mks6p0bv":
                        try:
                            value_str = col["value"] or "0"
                            if value_str.startswith('"') and value_str.endswith('"'):
                                value_str = value_str[1:-1]
                            leftover_numeric_value = float(value_str)
                            logger.info(f"Found original numeric_mks6p0bv value {leftover_numeric_value} from leftover subitem {subitem['name']}")
                        except (ValueError, TypeError):
                            leftover_numeric_value = 0
                        break
                break  # Exit the main loop since we found the leftover

        # Clear the leftover sum from the previous subitem if found
        if leftover_subitem_id and leftover_sum > 0:
            subitem_board_id = "9431861361"  # Default subitem board ID
            clear_response = update_subitem_column(leftover_subitem_id, "numeric_mksxv8ep", 0, subitem_board_id)
            if not clear_response:
                logger.error(f"Failed to clear leftover sum from source subitem {leftover_subitem_id}: response was None")
            elif clear_response.get("data", {}).get("change_column_value", {}).get("id"):
                logger.info(f"Successfully cleared leftover sum from source subitem {leftover_subitem_id}")
            else:
                logger.error(f"Failed to clear leftover sum from source subitem {leftover_subitem_id}: {clear_response}")


        # Distribute values across multiple subitems using sequential remainder logic
        # Start with leftover sum first, then add limit_value for subsequent processing
        remaining_value = leftover_sum
        total_to_distribute = leftover_sum + limit_value
        logger.info(f"Starting distribution with leftover_sum: {leftover_sum} + limit_value: {limit_value} = total: {total_to_distribute}")
        processed_subitems = []

        # Determine deduction column based on currency (is_dollar was set above)
        deduction_column = "numeric_mks6ywg8"  # Default to Euro column

        if is_dollar:
            deduction_column = "numeric_mks6myhs"

        logger.info(f"Starting sequential distribution from total {total_to_distribute} across {len(unprocessed_subitems)} eligible subitems")
        logger.info(f"Will process leftover first (if any) with value {leftover_numeric_value}, then continue with webhook value {numeric_value}")
        logger.info(f"Using deduction column: {deduction_column} for currency: {currency_dropdown} (is_dollar: {is_dollar})")

        # PHASE 1: Process leftover sum first (if any) - complete this phase entirely before webhook processing
        leftover_finished = False
        if leftover_sum > 0:
            logger.info(f"PHASE 1: Processing leftover sum {leftover_sum} with numeric value {leftover_numeric_value}")
            leftover_remaining = leftover_sum

            # Create a separate copy to avoid modification during iteration
            subitems_to_process = unprocessed_subitems[:]
            phase1_complete = False

            for subitem in subitems_to_process:
                if phase1_complete or leftover_remaining <= 0:
                    logger.info("Leftover sum exhausted, PHASE 1 complete")
                    leftover_finished = True
                    break

                # Get subitem deduction value from the appropriate column based on currency
                subitem_deduction_value = 0
                logger.debug(f"Subitem {subitem.get('name')} column values: {[{col['id']: col['value']} for col in subitem.get('column_values', [])]}")
                for col in subitem.get("column_values", []):
                    if col["id"] == deduction_column:
                        try:
                            raw_value = col["value"] or "0"
                            clean_value = raw_value.strip('"') if isinstance(raw_value, str) else str(raw_value)
                            subitem_deduction_value = float(clean_value)
                            logger.debug(f"Found {deduction_column} value: {col['value']} -> {subitem_deduction_value}")
                        except (ValueError, TypeError):
                            subitem_deduction_value = 0
                            logger.debug(f"Could not parse {deduction_column} value: {col['value']}")
                        break

                # Process leftover if there's a deduction value
                if subitem_deduction_value > 0:
                    subitem_board_id = subitem.get("board", {}).get("id", "9431861361")

                    if leftover_remaining >= subitem_deduction_value:
                        # Normal processing - leftover covers the deduction
                        update_response = update_subitem_column(subitem["id"], "numeric_mks6p0bv", leftover_numeric_value, subitem_board_id)
                        if update_response and update_response.get("data", {}).get("change_column_value", {}).get("id"):
                            leftover_remaining -= subitem_deduction_value
                        else:
                            logger.error(f"Failed to update subitem {subitem['name']}, skipping")
                            continue

                        processed_subitems.append({
                            "id": subitem["id"],
                            "name": subitem["name"],
                            "assigned_value": leftover_numeric_value,
                            "deducted_value": subitem_deduction_value,
                            "source": "leftover"
                        })

                        logger.info(f"PHASE 1: Processed subitem {subitem['name']}: copied {leftover_numeric_value} to numeric_mks6p0bv, deducted {subitem_deduction_value}, leftover remaining: {leftover_remaining}")

                        # Remove this specific subitem by ID from unprocessed list since it's done
                        unprocessed_subitems = [s for s in unprocessed_subitems if s["id"] != subitem["id"]]

                        if leftover_remaining <= 0:
                            leftover_finished = True
                            phase1_complete = True

                    else:
                        # Leftover is not enough - need to duplicate and split
                        logger.info(f"PHASE 1: Leftover remaining {leftover_remaining} is not enough for subitem {subitem['name']} deduction {subitem_deduction_value} - splitting")

                        # Duplicate the subitem to create Part 1 first, then Part 2
                        part1_id = duplicate_subitem(subitem["id"], f"{subitem['name']} Parte 1")
                        part2_id = duplicate_subitem(subitem["id"], f"{subitem['name']} Parte 2")

                        if part1_id and part2_id:
                            # Part 1 gets the remaining leftover and gets processed
                            update_response_part1_deduction = update_subitem_column(part1_id, deduction_column, leftover_remaining, subitem_board_id)
                            update_response_part1_numeric = update_subitem_column(part1_id, "numeric_mks6p0bv", leftover_numeric_value, subitem_board_id)

                            if not (update_response_part1_deduction and update_response_part1_deduction.get("data", {}).get("change_column_value", {}).get("id")) or \
                               not (update_response_part1_numeric and update_response_part1_numeric.get("data", {}).get("change_column_value", {}).get("id")):
                                logger.error(f"Failed to update Part 1 of split subitem {subitem['name']}")
                                phase1_complete = True  # Stop on error
                            else:
                                # Part 2 gets the difference (not processed yet, will be handled in PHASE 2)
                                part2_deduction = subitem_deduction_value - leftover_remaining
                                update_response_part2_deduction = update_subitem_column(part2_id, deduction_column, part2_deduction, subitem_board_id)

                                if not (update_response_part2_deduction and update_response_part2_deduction.get("data", {}).get("change_column_value", {}).get("id")):
                                    logger.error(f"Failed to update Part 2 of split subitem {subitem['name']}")
                                else:
                                    # Delete the original subitem
                                    if delete_item(subitem["id"], subitem_board_id):
                                        logger.info(f"Deleted original subitem {subitem['name']} (ID: {subitem['id']})")

                                    processed_subitems.append({
                                        "id": part1_id,
                                        "name": f"{subitem['name']} Parte 1",
                                        "assigned_value": leftover_numeric_value,
                                        "deducted_value": leftover_remaining,
                                        "source": "leftover"
                                    })

                                    # Add Part 2 to the beginning of unprocessed list for PHASE 2
                                    part2_subitem = {
                                        "id": part2_id,
                                        "name": f"{subitem['name']} Parte 2",
                                        "board": {"id": subitem_board_id},
                                        "column_values": [
                                            {"id": deduction_column, "value": f'"{part2_deduction}"'},
                                            {"id": "color_mks7xmpz", "value": subitem.get("column_values", [{}])[0].get("value", "")} # Assuming status is the first value
                                        ]
                                    }
                                    unprocessed_subitems.insert(0, part2_subitem)
                                    # Remove the original subitem by ID (not by object reference)
                                    unprocessed_subitems = [s for s in unprocessed_subitems if s["id"] != subitem["id"]]

                                    logger.info(f"PHASE 1: Created {subitem['name']} Parte 1 (processed) and Parte 2 (pending)")
                                    logger.info(f"Parte 1: {deduction_column}={leftover_remaining}, numeric_mks6p0bv={leftover_numeric_value}")
                                    logger.info(f"Parte 2: {deduction_column}={part2_deduction} - will be processed in PHASE 2")

                                    # Leftover is exhausted
                                    leftover_remaining = 0
                                    leftover_finished = True
                                    phase1_complete = True
                        else:
                            logger.error(f"Failed to duplicate subitem {subitem['name']}")
                            phase1_complete = True  # Stop on error
                else:
                    logger.info(f"PHASE 1: Skipped subitem {subitem.get('name')}: no deduction value")

            logger.info(f"PHASE 1 COMPLETE: Leftover processing finished. Remaining leftover: {leftover_remaining}")
        else:
            leftover_finished = True
            logger.info("PHASE 1 SKIPPED: No leftover sum to process")

        # PHASE 2: Process webhook values (only after leftover is completely finished)
        if leftover_finished and limit_value > 0:
            logger.info(f"PHASE 2: Processing webhook values with limit_value {limit_value} and numeric_value {numeric_value}")
            webhook_remaining = limit_value

            # Create a separate copy to avoid modification during iteration
            subitems_to_process = unprocessed_subitems[:]
            phase2_complete = False

            for subitem in subitems_to_process:
                if phase2_complete or webhook_remaining <= 0:
                    logger.info("Webhook limit_value exhausted, PHASE 2 complete")
                    break

                # Get subitem deduction value from the appropriate column based on currency
                subitem_deduction_value = 0
                for col in subitem.get("column_values", []):
                    if col["id"] == deduction_column:
                        try:
                            raw_value = col["value"] or "0"
                            clean_value = raw_value.strip('"') if isinstance(raw_value, str) else str(raw_value)
                            subitem_deduction_value = float(clean_value)
                        except (ValueError, TypeError):
                            subitem_deduction_value = 0
                        break

                # Process webhook if there's a deduction value
                if subitem_deduction_value > 0:
                    subitem_board_id = subitem.get("board", {}).get("id", "9431861361")

                    if webhook_remaining >= subitem_deduction_value:
                        # Normal processing - webhook value covers the deduction
                        update_response = update_subitem_column(subitem["id"], "numeric_mks6p0bv", numeric_value, subitem_board_id)
                        if update_response and update_response.get("data", {}).get("change_column_value", {}).get("id"):
                            webhook_remaining -= subitem_deduction_value
                        else:
                            logger.error(f"Failed to update subitem {subitem['name']}, skipping")
                            continue

                        processed_subitems.append({
                            "id": subitem["id"],
                            "name": subitem["name"],
                            "assigned_value": numeric_value,
                            "deducted_value": subitem_deduction_value,
                            "source": "webhook"
                        })

                        logger.info(f"PHASE 2: Processed subitem {subitem['name']}: copied {numeric_value} to numeric_mks6p0bv, deducted {subitem_deduction_value}, webhook remaining: {webhook_remaining}")

                        if webhook_remaining <= 0:
                            phase2_complete = True

                    else:
                        # Webhook remaining is not enough - need to duplicate and split
                        logger.info(f"PHASE 2: Webhook remaining {webhook_remaining} is not enough for subitem {subitem['name']} deduction {subitem_deduction_value} - splitting")

                        # Duplicate the subitem to create Part 1 first, then Part 2
                        part1_id = duplicate_subitem(subitem["id"], f"{subitem['name']} Parte 1")
                        part2_id = duplicate_subitem(subitem["id"], f"{subitem['name']} Parte 2")

                        if part1_id and part2_id:
                            # Part 1 gets the remaining webhook value and gets processed
                            update_response_part1_deduction = update_subitem_column(part1_id, deduction_column, webhook_remaining, subitem_board_id)
                            update_response_part1_numeric = update_subitem_column(part1_id, "numeric_mks6p0bv", numeric_value, subitem_board_id)

                            if not (update_response_part1_deduction and update_response_part1_deduction.get("data", {}).get("change_column_value", {}).get("id")) or \
                               not (update_response_part1_numeric and update_response_part1_numeric.get("data", {}).get("change_column_value", {}).get("id")):
                                logger.error(f"Failed to update Part 1 of split subitem {subitem['name']}")
                                phase2_complete = True  # Stop on error
                            else:
                                # Part 2 gets the difference (not processed)
                                part2_deduction = subitem_deduction_value - webhook_remaining
                                update_response_part2_deduction = update_subitem_column(part2_id, deduction_column, part2_deduction, subitem_board_id)

                                if not (update_response_part2_deduction and update_response_part2_deduction.get("data", {}).get("change_column_value", {}).get("id")):
                                    logger.error(f"Failed to update Part 2 of split subitem {subitem['name']}")
                                else:
                                    # Delete the original subitem
                                    if delete_item(subitem["id"], subitem_board_id):
                                        logger.info(f"Deleted original subitem {subitem['name']} (ID: {subitem['id']})")

                                    processed_subitems.append({
                                        "id": part1_id,
                                        "name": f"{subitem['name']} Parte 1",
                                        "assigned_value": numeric_value,
                                        "deducted_value": webhook_remaining,
                                        "source": "webhook"
                                    })

                                    processed_subitems.append({
                                        "id": part2_id,
                                        "name": f"{subitem['name']} Parte 2",
                                        "assigned_value": 0,  # Not processed
                                        "deducted_value": part2_deduction,
                                        "source": "pending"
                                    })

                                    logger.info(f"PHASE 2: Created {subitem['name']} Parte 1 (processed) and Parte 2 (pending)")
                                    logger.info(f"Parte 1: {deduction_column}={webhook_remaining}, numeric_mks6p0bv={numeric_value}")
                                    logger.info(f"Parte 2: {deduction_column}={part2_deduction} - not processed")

                                    # Webhook value is exhausted
                                    webhook_remaining = 0
                                    phase2_complete = True
                        else:
                            logger.error(f"Failed to duplicate subitem {subitem['name']}")
                            phase2_complete = True  # Stop on error
                else:
                    logger.info(f"PHASE 2: Skipped subitem {subitem.get('name')}: no deduction value")

            logger.info(f"PHASE 2 COMPLETE: Webhook processing finished. Remaining webhook value: {webhook_remaining}")
        else:
            webhook_remaining = 0
            logger.info("PHASE 2 SKIPPED: No webhook limit_value to process or leftover not finished")



        # Calculate remaining value based on the phases
        remaining_value = 0
        if leftover_finished and limit_value > 0:
            remaining_value = webhook_remaining
        elif leftover_sum > 0 and not leftover_finished:
            remaining_value = leftover_remaining

        # If there's a remaining value and we processed at least one subitem, save it to the last processed subitem
        if remaining_value > 0 and processed_subitems:
            last_processed = processed_subitems[-1]
            subitem_board_id = "9431861361"  # Default subitem board ID

            # Save the leftover sum to the numeric_mksxv8ep column of the last processed subitem
            update_response = update_subitem_column(last_processed["id"], "numeric_mksxv8ep", remaining_value, subitem_board_id)
            if update_response and update_response.get("data", {}).get("change_column_value", {}).get("id"):
                logger.info(f"Saved leftover sum {remaining_value} to subitem {last_processed['name']} (ID: {last_processed['id']}) in column numeric_mksxv8ep")
            else:
                logger.error(f"Failed to save leftover sum to subitem {last_processed['name']}")

        # Store operation state
        operation_state[item_id] = {
            "timestamp": datetime.now().isoformat(),
            "processed_subitems": processed_subitems,
            "remaining_value": remaining_value,
            "leftover_saved": remaining_value > 0 and processed_subitems
        }

        return {
            "message": "Values distributed successfully",
            "processed_subitems": processed_subitems,
            "remaining_value": remaining_value,
            "leftover_saved_to_subitem": processed_subitems[-1]["name"] if remaining_value > 0 and processed_subitems else None
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
    Webhook endpoint for updating reserve values and distributing
    Checks for RESERVA values in subitems and transfers them before distribution
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
        subitem_id = None

        # Handle Monday.com webhook format
        if "event" in payload and "parentItemId" in payload["event"]:
            parent_item_id = payload["event"]["parentItemId"]
            subitem_id = payload["event"].get("pulseId", "")
        elif "event" in payload and "data" in payload["event"]:
            # Current webhook format with nested data
            event_data = payload["event"]["data"]
            parent_item_id = event_data.get("parent_item", "")
            subitem_id = event_data.get("item_id", "")
        elif "parent_item" in payload:
            parent_item_id = payload["parent_item"].get("id", "")
            subitem_id = payload.get("id", "")
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

        # Query Monday.com to get the actual parent item data with required columns
        item_data = get_item_data(parent_item_id, item_name)

        if not item_data or not isinstance(item_data, dict): # Added check for dict type
            logger.error("Could not retrieve parent item data or data is not a dictionary")
            return jsonify({"error": "Could not retrieve parent item data"}), 400

        # Get subitems from the parent item with same name in group_mks6z9xe
        group_id = "group_mks6z9xe"
        logger.info(f"Looking for parent item '{item_name}' in group '{group_id}' to check for RESERVA values")
        subitems = get_subitems_by_group_and_name(group_id, item_name)

        if not subitems:
            logger.warning(f"No subitems found for parent item '{item_name}' in group '{group_id}'")
            return {"message": f"No subitems found for parent item '{item_name}' in group '{group_id}'"}, 200

        # Check for RESERVA values (numeric_mksxv8ep) in subitems
        reserva_found = False
        reserva_value = 0
        source_numeric_value = 0
        source_subitem_id = None

        for subitem in subitems:
            current_reserva = 0
            current_numeric = 0

            for col in subitem.get("column_values", []):
                if col["id"] == "numeric_mksxv8ep":  # RESERVA column
                    try:
                        value_str = col["value"] or "0"
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        current_reserva = float(value_str)
                    except (ValueError, TypeError):
                        current_reserva = 0
                elif col["id"] == "numeric_mks6p0bv":  # Source numeric value
                    try:
                        value_str = col["value"] or "0"
                        if value_str.startswith('"') and value_str.endswith('"'):
                            value_str = value_str[1:-1]
                        current_numeric = float(value_str)
                    except (ValueError, TypeError):
                        current_numeric = 0

            if current_reserva > 0:
                reserva_found = True
                reserva_value = current_reserva
                source_numeric_value = current_numeric
                source_subitem_id = subitem["id"]
                logger.info(f"Found RESERVA {reserva_value} in subitem {subitem['name']} with numeric value {source_numeric_value}")
                break

        if not reserva_found:
            logger.info("No RESERVA values found in subitems - nothing to do")
            return jsonify({"message": "No RESERVA values found in subitems"}), 200

        # Transfer the numeric value from the subitem with RESERVA to the webhook item
        logger.info(f"Transferring numeric value {source_numeric_value} from subitem to webhook item {item_name}")

        # Update the webhook item's numeric_mks6p0bv column with the source value
        update_query = """
        mutation($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
            change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
                id
            }
        }
        """

        update_variables = {
            "boardId": "9431708170",  # Parent item board
            "itemId": str(parent_item_id),
            "columnId": "numeric_mks6p0bv",
            "value": str(source_numeric_value)
        }

        update_response = make_monday_api_request(update_query, update_variables)

        if not update_response or not isinstance(update_response, dict):
            logger.error(f"Invalid response from Monday API for update: {update_response}")
            return jsonify({"error": "Failed to transfer value to webhook item"}), 500

        if not update_response.get("data", {}).get("change_column_value", {}).get("id"):
            logger.error(f"Failed to update webhook item {item_name} with transferred value")
            return jsonify({"error": "Failed to transfer value to webhook item"}), 500

        logger.info(f"Successfully transferred value {source_numeric_value} to webhook item {item_name}")

        # Clear the RESERVA value from the source subitem
        subitem_board_id = "9431861361"  # Default subitem board ID
        try:
            logger.info(f"About to clear RESERVA value from subitem {source_subitem_id} using board {subitem_board_id}")
            clear_response = update_subitem_column(source_subitem_id, "numeric_mksxv8ep", 0, subitem_board_id)
            logger.info(f"Clear RESERVA response received: {clear_response}")
            
            if clear_response is None:
                logger.error(f"Failed to clear RESERVA value from source subitem {source_subitem_id}: response was None")
            elif not isinstance(clear_response, dict):
                logger.error(f"Failed to clear RESERVA value from source subitem {source_subitem_id}: response is not a dict: {type(clear_response)}")
            elif clear_response.get("data", {}).get("change_column_value", {}).get("id"):
                logger.info(f"Successfully cleared RESERVA value from source subitem {source_subitem_id}")
            else:
                logger.error(f"Failed to clear RESERVA value from source subitem {source_subitem_id}: {clear_response}")
        except Exception as e:
            logger.error(f"Failed to clear RESERVA value: {e}")
            import traceback
            logger.error(f"Clear RESERVA traceback: {traceback.format_exc()}")
            # Continue anyway, as the main transfer was successful

        # Update item_data with the transferred value to use in distribution
        item_data["numeric_mks63qc1"] = source_numeric_value

        # Ensure item_data has all required fields for distribution
        required_fields = ["id", "name", "numeric_mks63qc1", "numeric_mks64nh2", "color_mks7xywc", "numeric_mks61nvq"]
        missing_fields = [field for field in required_fields if field not in item_data]

        if missing_fields:
            logger.error(f"Item data missing fields for distribution: {missing_fields}. Current item_data: {item_data}")
            return jsonify({
                "error": f"Item data missing required fields: {missing_fields}",
                "transferred_value": source_numeric_value,
                "reserva_cleared": reserva_value
            }), 500

        # Validate that we have the required data for distribution (status column must have a value)
        if not item_data.get("color_mks7xywc"):
            logger.info(f"Item {item_name} has no status value set, skipping distribution after transfer")
            return jsonify({
                "message": "Value transferred but no status value set for distribution",
                "transferred_value": source_numeric_value,
                "reserva_cleared": reserva_value
            }), 200

        if item_data.get("numeric_mks63qc1", 0) <= 0:
            logger.info(f"Item {item_name} has no value to distribute after transfer")
            return jsonify({
                "message": "Value transferred but no value to distribute",
                "transferred_value": source_numeric_value,
                "reserva_cleared": reserva_value
            }), 200

        logger.info(f"Starting distribution with item_data: {item_data}")

        # Process the distribution using the same logic as /distribuir
        try:
            distribution_result = distribute_values(item_data)

            # Handle the case where distribute_values returns None or invalid data
            if distribution_result is None:
                logger.error("distribute_values returned None")
                return jsonify({
                    "error": "Distribution function returned None",
                    "transferred_value": source_numeric_value,
                    "reserva_cleared": reserva_value
                }), 500

            # distribute_values returns a tuple (result, status_code)
            if not isinstance(distribution_result, tuple) or len(distribution_result) != 2:
                logger.error(f"distribute_values returned invalid format: {type(distribution_result)}")
                return jsonify({
                    "error": "Distribution function returned invalid format",
                    "transferred_value": source_numeric_value,
                    "reserva_cleared": reserva_value
                }), 500

            result, status_code = distribution_result

            # Validate the result part
            if result is None:
                logger.error("distribute_values returned None as result")
                return jsonify({
                    "error": "Distribution function returned None result",
                    "transferred_value": source_numeric_value,
                    "reserva_cleared": reserva_value
                }), 500

        except Exception as e:
            logger.error(f"Error in distribute_values: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({
                "error": f"Distribution failed: {str(e)}",
                "transferred_value": source_numeric_value,
                "reserva_cleared": reserva_value
            }), 500

        # Add transfer information to the result
        if isinstance(result, dict):
            result["transferred_from_reserva"] = {
                "source_subitem_id": source_subitem_id,
                "transferred_value": source_numeric_value,
                "reserva_cleared": reserva_value
            }
        else:
            logger.error(f"Expected dict result from distribute_values, got: {type(result)}")
            return jsonify({
                "error": "Invalid result from distribution",
                "result_type": str(type(result)),
                "transferred_value": source_numeric_value,
                "reserva_cleared": reserva_value
            }), 500

        return jsonify(result), status_code

    except Exception as e:
        logger.error(f"Unexpected error in /atualizarreservadecambio endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/distribuir', methods=['POST'])
def distribuir():
    """
    Main webhook endpoint for Monday.com payloads
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

        if not item_data or not isinstance(item_data, dict): # Added check for dict type
            logger.error("Could not retrieve item data or data is not a dictionary")
            return jsonify({"error": "Could not retrieve item data"}), 400

        # Validate that we have the required data (status column must have a value)
        if not item_data.get("color_mks7xywc"):
            logger.info(f"Item {item_name} has no status value set, skipping processing")
            return jsonify({"message": "No status value set, skipping processing"}), 200

        if item_data.get("numeric_mks63qc1", 0) <= 0:
            logger.info(f"Item {item_name} has no value to distribute")
            return jsonify({"message": "No value to distribute"}), 200

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