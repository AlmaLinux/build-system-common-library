import enum

COMMUNITY_KEY_SUFFIX = 'ALBS community repo'

# Max file size to allow parallel upload for
DEFAULT_PARALLEL_FILE_UPLOAD_SIZE = 52428800  # 500 MB


class SignStatusEnum(enum.IntEnum):
    SUCCESS = 1
    READ_ERROR = 2
    NO_SIGNATURE = 3
    WRONG_SIGNATURE = 4
