from typing import Optional


class BagelError(Exception):
    def __init__(self, errors):
        spacer = "---------------------------"
        self.message = f"Errors occured in Bagel:\n{spacer}\n" + f"\n{spacer}\n".join(
            errors
        )
        super().__init__(self.message)
