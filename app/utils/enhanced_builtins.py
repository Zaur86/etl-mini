from datetime import datetime, timezone


class DynamicTimeDict(dict):
    """
    A custom dictionary class where a specific key dynamically returns
    the current time in the format 'YYYY-MM-DD HH:MM:SS'.
    """

    def __init__(self, dynamic_key, *args, **kwargs):
        """
        Initialize the dictionary and register the dynamic key.

        Args:
            dynamic_key (str): The key that will always return the current time.
            *args: Positional arguments for the parent dict.
            **kwargs: Keyword arguments for the parent dict.
        """
        super().__init__(*args, **kwargs)
        self.dynamic_key = dynamic_key
        # Explicitly add the dynamic key to the dictionary
        self[dynamic_key] = None

    def __getitem__(self, key):
        """
        Override __getitem__ to return the current time for the dynamic key.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            str: The current time in 'YYYY-MM-DD HH:MM:SS' format for the dynamic key.
            For other keys, it returns the stored value.
        """
        if key == self.dynamic_key:
            return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        return super().__getitem__(key)

    def keys(self):
        """
        Override keys() to ensure the dynamic key is included in the key list.

        Returns:
            dict_keys: A view of all keys in the dictionary.
        """
        return super().keys()

    def items(self):
        """
        Override items() to include the dynamic key with its dynamic value.

        Returns:
            generator: A generator of key-value pairs.
        """
        for key in super().keys():
            yield key, self[key]
