class MissingKeywordArg(Exception):
    def __init__(self, keyword):
        self.keyword = keyword

    def __str__(self):
        return "Keyword argument '{}' was missing".format(self.keyword)


# Usage:
# @require_keywords_args(['keyword1', 'keyword2', ...])
# def __init__(self, *args, **kwargs):
class require_keyword_args:
    def __init__(self, required_keywords):
        self.required_keywords = required_keywords

    def __call__(self, func):
        def required_func(*args, **kwargs):
            for keyword in self.required_keywords:
                if keyword not in kwargs:
                    raise MissingKeywordArg(keyword)
            return func(*args, **kwargs)

        return required_func
