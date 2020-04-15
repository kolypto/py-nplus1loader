import json
from exdoc import doc

# Data
import nplusoneloader

data = dict(
    module=doc(nplusoneloader),
    # load_options = [
    #     doc(nplusoneloader.)
    # ]
)

# Document
print(json.dumps(data, indent=2))
