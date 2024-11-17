"""
My object can be created in different ways
"""

class Request(object):
    def __init__(self, url: str):
        self.url = url

    @classmethod
    def from_url(cls, url: str):
        stripped = url.strip('//')
        return Request(stripped)

def test_create_with_alternative():
    regular = Request('a')
    non = Request.from_url('//a')
    assert isinstance(regular, Request)
    assert isinstance(non, Request)

