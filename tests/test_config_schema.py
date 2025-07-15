import pytest
from pydantic import ValidationError

from tlptaco.config.schema import (
    ConditionCheck,
    TemplateConditions,
    ConditionsConfig,
    TableConfig,
    OutputChannelConfig,
    EligibilityConfig,
)


def make_template_conditions():
    # create a BA filter and at least one dummy segment for testing
    ba = [ConditionCheck(name="chk", sql="1=1")]
    # legacy 'others' used to create a non-empty segment mapping
    others = {"seg1": [ConditionCheck(name="chk2", sql="1=1")]}  # dummy segment
    return TemplateConditions(BA=ba, others=others)


def make_conditions_config():
    base = make_template_conditions()
    return ConditionsConfig(main=base, channels={"ch": make_template_conditions()})


def test_output_unique_on_validation_fails():
    with pytest.raises(ValidationError):
        OutputChannelConfig(
            columns=["a", "b"],
            file_location=".",
            file_base_name="fn",
            output_options={"format": "csv"},
            unique_on=["c"],
        )

def test_invalid_eligibility_table_name():
    # eligibility_table must be a valid identifier or schema.table
    with pytest.raises(ValidationError):
        EligibilityConfig(
            eligibility_table="123bad",
            conditions=make_conditions_config(),
            tables=[TableConfig(name="t", alias="t", sql=None,
                                join_type=None, join_conditions=None,
                                where_conditions=None, unique_index=None,
                                collect_stats=None)],
            unique_identifiers=["t.id"],
        )


def test_invalid_unique_identifiers_alias():
    tables = [TableConfig(name="t", alias="x", sql=None, join_type=None, join_conditions=None, where_conditions=None, unique_index=None, collect_stats=None)]
    with pytest.raises(ValidationError):
        EligibilityConfig(
            eligibility_table="e_table",
            conditions=make_conditions_config(),
            tables=tables,
            unique_identifiers=["y.id"],
        )
    # table alias must start with letter or underscore
    with pytest.raises(ValidationError):
        TableConfig(
            name="t", alias="1bad", sql=None,
            join_type=None, join_conditions=None, where_conditions=None,
            unique_index=None, collect_stats=None
        )
