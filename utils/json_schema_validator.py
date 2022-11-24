import json
from jsonschema import validate, Draft202012Validator


def validate_result_structure(instance, schema):
    """
    Validate forecast tasks result schema against expected schema
    """

    instance = json.loads(s=instance)
    validate(
        instance=instance,
        schema=schema,
        cls=Draft202012Validator,
    )
