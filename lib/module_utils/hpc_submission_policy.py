class HPCSubmissionPolicy:
    """
    Aggregates multiple signals to decide HPC submission:
      - user_manual_submit: True means forced manual HPC,
                            False means no forced manual (auto allowed),
                            None if not set or unknown.
      - doc_auto_submit:    True if the project doc says auto-submission,
                            False if doc says manual,
                            default True if doc is missing the field.
      - realm_supports_auto: True if realm actually supports auto-submission,
                             False if realm is partially developed => forced manual.

    The final decision is:
      final_auto = doc_auto_submit AND realm_supports_auto AND (NOT user_manual_submit)
      => If user_manual_submit is True => forced manual.
         If doc_auto_submit is False => forced manual.
         If realm_supports_auto is False => forced manual.
    """

    def __init__(
        self,
        user_manual_submit: bool | None = None,
        doc_auto_submit: bool = True,
        realm_supports_auto: bool = False,
    ):
        """
        If user_manual_submit is True => user forced manual HPC.
           If None => no user override => default to auto if doc & realm say so.
        doc_auto_submit: from the yggdrasilDB doc, defaults True if not specified.
        realm_supports_auto: from the realm (if it can handle auto HPC).
        """
        self._user_manual_submit = user_manual_submit
        self._doc_auto_submit = doc_auto_submit
        self._realm_supports_auto = realm_supports_auto

    @property
    def user_manual_submit(self) -> bool | None:
        return self._user_manual_submit

    @user_manual_submit.setter
    def user_manual_submit(self, val: bool | None):
        self._user_manual_submit = val

    @property
    def doc_auto_submit(self) -> bool:
        return self._doc_auto_submit

    @doc_auto_submit.setter
    def doc_auto_submit(self, val: bool):
        self._doc_auto_submit = val

    @property
    def realm_supports_auto(self) -> bool:
        return self._realm_supports_auto

    @realm_supports_auto.setter
    def realm_supports_auto(self, val: bool):
        self._realm_supports_auto = val

    def should_auto_submit(self) -> bool:
        """
        Return True if HPC auto-submission is permitted, else False for manual.
        The logic is:
         - forced manual if user_manual_submit == True
         - forced manual if doc_auto_submit == False
         - forced manual if realm_supports_auto == False
         - otherwise => auto
        """
        # If user forced manual
        if self._user_manual_submit is True:
            return False
        # If doc says manual
        if not self._doc_auto_submit:
            return False
        # If realm can't auto-submit
        if not self._realm_supports_auto:
            return False

        # If all conditions are good => auto
        return True
