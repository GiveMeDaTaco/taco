from tlptaco.validations.exceptions import ValueWarning
from tlptaco.validations.eligibility import EligibleValidator
import os


class WaterfallValidator:
    """
    Contains various validations for `tlptaco.waterfall`
    """
    _validators = {}

    @staticmethod
    def validate_conditions(conditions):
        ...

    @staticmethod
    def validate_waterfall_location(waterfall_location):
        if not isinstance(waterfall_location, str):
            raise ValueError(f"waterfall_location should be a string, instead it was {type(waterfall_location)}")
        if not os.path.isdir(waterfall_location):
            raise ValueError(f"the waterfall_location {waterfall_location} does not exist")

    @staticmethod
    def validate_unique_identifiers(unique_identifiers):
        return EligibleValidator.validate_unique_identifiers(unique_identifiers)

    @staticmethod
    def validate_campaign_planner(value):
        if value is None:
            raise ValueError(
                "f'campaign_planner' is 'None'; attempting to build a waterfall from this instance of 'Eligible' will fail")
        elif not isinstance(value, str):
            raise ValueError(
                f"campaign_planner is type {type(value)}, instead of a string; attempting to build a waterfall from this instance of Eligible will fail")
        elif value == '':
            raise ValueError(
                f"'campaign_planner' is blank; attempting to build a waterfall from this instance of 'Eligible' will fail")

    @staticmethod
    def validate_lead(value):
        if value is None:
            raise ValueError(
                f"'lead' is 'None'; it must be a string; attempting to build a waterfall from this instance of Eligible will fail")
        elif not isinstance(value, str):
            raise ValueError(
                f"lead is type {type(value)}, instead of a string; attempting to build a waterfall from this instance of Eligible will fail")
        elif value == '':
            raise ValueError(
                f"'lead' is blank; attempting to build a waterfall from this instance of Eligible will fail")

    @staticmethod
    def validate_username(value):
        if value is None:
            raise ValueError(f"'username' cannot be 'None'; it must be a string")
        elif not isinstance(value, str):
            raise ValueError(f"username must be a string, not {type(value)}")
        elif value == '':
            raise ValueError(f"'username' cannot be blank")

    @staticmethod
    def validate_offer_code(value):
        if value is None:
            raise ValueError(
                f"'offer_code' is 'None'; attempting to build a waterfall from this instance of 'Eligible' will fail")
        elif not isinstance(value, str):
            raise ValueError(
                f"offer_code is type {type(value)}, not string; attempting to build a waterfall from this instance of 'Eligible' will fail")
        elif value == '':
            raise ValueError(
                f"'offer_code' is blank; attempting to build a waterfall from this instance of 'Eligible' will fail")