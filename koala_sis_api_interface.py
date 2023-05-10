"""The TestDataClient returns the test data associated with the provided credentials."""


class KoalaSisDataClient:
    def __init__(self, credentials):
        """
        Constructor for the client.
        :param credentials: A credentials string
        """
        raise NotImplementedError

    def get_student_data(self, sort_order=None, batch_size=None, records_limit=None):
        """
        Gets data about students in the Koala SIS

        :param sort_order: 'asc' or 'desc'
        :param batch_size: how many items returned at once
        :param records_limit: how many items returned total

        :return: A generator yielding each batch of records.
        """
        raise NotImplementedError

    def get_schools_data(self, sort_order=None, batch_size=None, records_limit=None):
        """
        Gets data about schools in the Koala SIS

        :param sort_order: 'asc' or 'desc'
        :param batch_size: how many items returned at once
        :param records_limit: how many items returned total

        :return: A generator yielding each batch of records.
        """
        raise NotImplementedError

    def get_enrollment_data(self, sort_order=None, batch_size=None, records_limit=None):
        """
        Gets data about student enrollment records in the Koala SIS

        :param sort_order: 'asc' or 'desc'
        :param batch_size: how many items returned at once
        :param records_limit: how many items returned total

        :return: A generator yielding each batch of records.
        """
        raise NotImplementedError
