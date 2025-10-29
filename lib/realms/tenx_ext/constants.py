from enum import StrEnum


class Role(StrEnum):
    LIBRARIES_CSV = "libraries_csv"


class DirName:
    LIBS = "libraries"
    CR_OUTS = "cr_outs"


class FileName:
    LIBRARIES = "libraries.csv"
