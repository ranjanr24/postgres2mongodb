

def get_file_contents_as_string(path):
    """
    This method reads data from a file and returns the file
    contents as a string.
    @param : absolute path of the file
    """
    with open(path, 'r') as my_file:

        data = my_file.read().replace('\n', '')

    return data


def get_file_contents(path):
    """
    This method is same as the above but doesnt strip down the new line.
    :param path:
    :return:
    """

    with open(path, 'r') as my_file:

        data = my_file.read()

    return data
