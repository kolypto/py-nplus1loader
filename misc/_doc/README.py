import json
from exdoc import doc

# Data
import nplus1loader

data = dict(
    module=doc(nplus1loader),
    # load_options = [
    #     doc(nplus1loader.)
    # ]
)

# Document
print(json.dumps(data, indent=2))
