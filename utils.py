def deep_get_attr(obj, k):
    keys = k.split('.')
    attr = obj

    while keys:
        k = keys.pop(0)
        attr = attr.get(k, None)

    return attr


def validate_command(msg):
    i = raw_input(
        '{}, please confirm (y/n): '.format(msg))

    if not i == 'y':
        raise ValueError('Command aborted')

    return True
