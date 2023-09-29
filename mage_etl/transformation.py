if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from datetime import datetime
import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df = data

    def map_aqi_to_description(aqi):
        if aqi == 1:
            return 'Good'
        elif aqi == 2:
            return 'Fair'
        elif aqi == 3:
            return 'Moderate'
        elif aqi == 4:
            return 'Poor'
        elif aqi == 5:
            return 'Very Poor'
        else:
            return 'Unknown'

    df['Qualitative_Description'] = df['aqi'].apply(map_aqi_to_description)
    df['date_timestamp'] = pd.to_datetime(datetime.now())


    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
