from sqlalchemy.exc import InvalidRequestError


class LazyLoadingAttributeError(InvalidRequestError):
    """ An attribute is being lazy-loaded while raiseload is in effect """

    attribute_name: str

    def __init__(self, model_name, attribute_name):
        self.model_name = model_name
        self.attribute_name = attribute_name
        super().__init__(f"{self.model_name}.{self.attribute_name} is not available due to raiseload")
