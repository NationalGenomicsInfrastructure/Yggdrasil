class YggSession:
    """
    Manages session-wide flags and settings, like dev mode and manual HPC submission.

    Once set, these cannot be changed mid-run, to avoid inconsistent states.
    """

    __dev_mode = False  # Development mode flag
    __dev_already_set = False  # Flag to prevent changing the mode mid-run

    __manual_submit = False
    __manual_already_set = False

    @classmethod
    def init_dev_mode(cls, dev: bool):
        """
        Sets the development mode if it has not been set before.
        Raises a RuntimeError when the mode is already set.
        """
        if cls.__dev_already_set:
            # raise an exception or ignore repeated calls
            raise RuntimeError("Dev mode was already set. Cannot change mid-run.")
        cls.__dev_mode = dev
        cls.__dev_already_set = True

    @classmethod
    def is_dev(cls) -> bool:
        """
        Returns True if the application is in development mode, otherwise False.
        """
        return cls.__dev_mode

    @classmethod
    def init_manual_submit(cls, manual: bool):
        """
        Sets the manual HPC submission flag if it has not been set before.
        Raises a RuntimeError when the flag is already set.
        """
        if cls.__manual_already_set:
            raise RuntimeError(
                "HPC submission flag already set; cannot change mid-run."
            )
        cls.__manual_submit = manual
        cls.__manual_already_set = True

    @classmethod
    def is_manual_submit(cls) -> bool:
        """
        Returns True if the application is in manual HPC submission mode, otherwise False.
        """
        return cls.__manual_submit
