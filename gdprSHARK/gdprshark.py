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


def fetch_data(object_class, fetch_features, logger, max_objects=None, chunk_size=None):
    """Fetch a dataframe from a smartSHARK database.

    Args:
        object_class (pycoshark.mongomodel): database object typ to fetch
        fetch_features (list(str)): features to fetch from the database
        logger: Logging object
        max_objects (int): maximum number of objects to fetch
        chunk_size (int): size after which the current state is logged

    Returns:
        (list[attributes]): fetched data
    """
    start_time = time.time()
    i, chunk_count = 0, 0
    fetch_list = []
    for obj in object_class.objects().only(*fetch_features):
        attr_list = []
        for feature in fetch_features:
            attr_list.append(getattr(obj, feature))
        fetch_list.append(attr_list)
        i += 1
        if chunk_size is not None:
            if i % chunk_size == 0:
                chunk_count += 1
                logger.info(f'fetched {chunk_count*chunk_size} elements...')
        if max_objects is not None:
            if i >= max_objects:
                logger.info(f"reached max number of objects limit")
                break

    logger.info(f"fetched {len(fetch_list)} elements in {time.time() - start_time:.1f} s")
    return fetch_list


def create_and_clean_email_dict(email_list):
    """Cleans emails by setting to lower case, filtering with regular expression and dropping all duplicate addresses.

    Args:
        email_list (list): contains ids of persons and the according email addresses

    Returns:
        (dict): dictionary with the cleaned email addresses as key and the id as value
    """
    id_list = [i[0] for i in email_list]
    extracted_addresses = [re.findall(get_email_re(), i[1].lower()) for i in email_list]

    duplicate_list = set()
    addr_dict = dict()
    for people_id, addr in zip(id_list, extracted_addresses):
        if not addr:
            pass
        else:
            addr = addr[0]
            if addr in duplicate_list:
                continue
            else:
                if addr in addr_dict:
                    duplicate_list.add(addr)
                    addr_dict.pop(addr)
                else:
                    addr_dict[addr] = people_id
    return addr_dict


def filter_email_addresses(text):
    """Returns all email address regular expressions found in the document or None if none are found."""
    if re.findall(get_email_re(), text):
        return re.findall(get_email_re(), text)
    else:
        return None


def find_and_replace_email_single_file(text, email_dict):
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


def find_and_replace_email(object_class, field_name, text_list, email_dict, logger):
    """Wrapper around find_and_replace_email_single_file function to find and replace regular expressions in a list of documents.

    Args:
        object_class (pycoshark.mongomodel): class of pycoshark object to update
        field_name (str): name of the field to look for email addresses
        text_list (list(str)): list of documents to be searched for addresses
        email_dict (dict): dictionary with address as key and person id as value
        logger: Logging object

    Returns:
        (tuple):
            - list of documents containing the replacement tokens
            - count for email regular expressions found in all documents
            - count for the email regular expressions replaced by a token from the dictionary in all documents
    """
    start_time = time.time()
    overall_found_counter = 0
    overall_replace_counter = 0
    updated_text_list = []
    for document_id, text in text_list:
        if text is not None:
            updated_text, temp_found_counter, temp_replace_counter = find_and_replace_email_single_file(text, email_dict)
            if temp_found_counter > 0:
                doc = object_class.objects(id=document_id)
                update_field = {field_name: updated_text}
                doc.update(**update_field)
            updated_text_list.append(updated_text)
            overall_found_counter += temp_found_counter
            overall_replace_counter += temp_replace_counter

    logger.info("replacement statistics:")
    logger.info(f"objects:         {len(text_list)}")
    logger.info(f"found emails:    {overall_found_counter}")
    logger.info(f"replaced emails: {overall_replace_counter}")
    logger.info(f"time needed:     {time.time() - start_time:.3f} s")

    return updated_text_list, overall_found_counter, overall_replace_counter
