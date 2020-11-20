import os
_version = os.environ.get("CI_COMMIT_TAG", "1.2.0.dev6")
print(_version)
