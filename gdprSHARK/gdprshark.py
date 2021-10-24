import math
import re
import time


def get_email_re():
    """Returns a regular expression describing a valid email address.

    Note: The regular expression does not contain every possible valid email address.
    This is to avoid issues with the embedding in the document (e.g. '/' left out)
    and for the sake of simplicity (e.g. no Chinese letters).
    """
    local_part = r'[a-zA-Z0-9!#$%&‘*+=?^_`.{|}~-]{1,64}'
    domain_part = r'[a-zA-Z0-9.-]{1,253}\.[a-zA-Z]{1,63}'
    return f'({local_part}@{domain_part})'


def load_email_dict(db, logger):
    """Fetches email addresses and person ids from database, filters for valid email addresses,
    and returns a dict with the email address as key and the person id(s) as value.

    If there are multiple entries in the people collection associated with the same email address,
    the value in the dict is not one single id, but a comma-separated string with all the associated ids.
    For more than 10 associated entries, the email address is considered a non-personal address and not filtered anymore.

    Args:
        db (MongoClient): mongoDB database handle
        logger: logging object

    Returns:
        (dict(str,str)): dictionary with the cleaned email addresses as key and the id(s) as value
    """
    element_ids = [element['_id'] for element in db['people'].find(no_cursor_timeout=True)]
    logger.info(f"start loading email addresses ({len(element_ids)} people total)")
    email_list = []
    for i in range(0, math.ceil(len(element_ids) / 100)):
        slice_start = i * 100
        slice_end = min((i + 1) * 100, len(element_ids))
        cur_element_slice = element_ids[slice_start:slice_end]
        if db['people'].count_documents({'_id': {'$in': cur_element_slice}}) > 0:
            data = db['people'].find({'_id': {'$in': cur_element_slice}}, no_cursor_timeout=True)
            for instance in data:
                if 'email' in instance:
                    email_list.append([instance['_id'], instance['email']])

    if not email_list:
        error_msg = "Not able to load email addresses."
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    id_list = [str(i[0]) for i in email_list]
    extracted_addresses = [re.findall(get_email_re(), i[1].lower()) for i in email_list]
    ten_duplicates_list = set()
    address_dict = dict()
    for people_id, address in zip(id_list, extracted_addresses):
        if not address:  # if the re finds no valid email in the email field
            pass
        else:
            address = address[0]  # if the re finds more than one valid email in the email field
            if address in ten_duplicates_list:
                continue
            else:
                if address in address_dict:
                    if address_dict[address].count(",") >= 9:
                        ten_duplicates_list.add(address)
                        address_dict.pop(address)
                    else:
                        address_dict[address] = ",".join((address_dict[address], people_id))
                else:
                    address_dict[address] = people_id
    return address_dict


def update_db_with_email_filter(db, collection_name, field_name, email_dict, logger):
    """Fetches a collection from the database and replaces email addresses batch-wise for one field.

    Args:
        db (MongoClient): mongoDB database handle
        collection_name (str): name of the collection to update
        field_name (str): name of the field to update
        email_dict (dict(str, str)): dictionary with address as key and person id as value
        logger: logging object
    """
    start_time = time.time()
    overall_documents_count = 0
    overall_found_count = 0
    overall_replace_count = 0
    element_ids = [element['_id'] for element in db[collection_name].find(no_cursor_timeout=True)]
    logger.info(f"start loading '{collection_name}' and replacing email addresses in the field '{field_name}' "
                f"({len(element_ids)} '{collection_name}' total)")

    for i in range(0, math.ceil(len(element_ids) / 100)):
        slice_start = i * 100
        slice_end = min((i + 1) * 100, len(element_ids))
        cur_element_slice = element_ids[slice_start:slice_end]
        if db[collection_name].count_documents({'_id': {'$in': cur_element_slice}}) > 0:
            data = db[collection_name].find({'_id': {'$in': cur_element_slice}}, no_cursor_timeout=True)
            for instance in data:
                if field_name in instance:
                    updated_text, found_count, replace_count = find_and_replace_email(instance[field_name], email_dict)
                    overall_documents_count += 1
                    overall_found_count += found_count
                    overall_replace_count += replace_count
                    if replace_count > 0:
                        db[collection_name].update({"_id": instance['_id']},
                                                   {"$set": {field_name: updated_text}})
    logger.info("replacement statistics:")
    logger.info(f"searched fields: {overall_documents_count}")
    logger.info(f"found emails:    {overall_found_count}")
    logger.info(f"replaced emails: {overall_replace_count}")
    logger.info(f"time needed:     {time.time() - start_time:.3f} s")
    if overall_documents_count == 0:
        logger.error(f"No documents with the field '{field_name}' found in the collection '{collection_name}'.")


def find_and_replace_email(text, email_dict):
    """Searches in a string for email regular expressions.
    If there are findings these are looked for in the address dictionary and replaced by a token if there is a match.

    Args:
        text (str): document to be searched for addresses
        email_dict (dict): dictionary with address as key and person id as value

    Returns:
        (tuple):
            - document containing the replacement tokens
            - count for email regular expressions found in the document
            - count for the email regular expressions replaced by a token from the dictionary
    """
    found_counter, replace_counter = 0, 0
    filtered_addresses = filter_email_addresses(text)
    if filtered_addresses is not None:
        found_counter += len(filtered_addresses)
        filtered_addresses = sorted(set(filtered_addresses), key=len, reverse=True)

        for address in filtered_addresses:
            if address.lower() in email_dict:
                replacement_string = f"[email:{email_dict[address.lower()]}]"
                text, current_replace_counter = re.subn(address, replacement_string, text)
                replace_counter += current_replace_counter

    return text, found_counter, replace_counter


def filter_email_addresses(text):
    """Returns all email address regular expressions found in the document or None if none are found."""
    if re.findall(get_email_re(), text):
        return re.findall(get_email_re(), text)
    else:
        return None
