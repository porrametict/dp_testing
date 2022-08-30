from app.dataprepare.thai_langueage_helper import parse_alien_language


def parse_to_text(r):

    text_data = parse_alien_language(r)
    parsed_data = text_data
    schema = None
    return (schema, text_data, parsed_data)
