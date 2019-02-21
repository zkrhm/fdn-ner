# # import language-specific data
# from .stop_words import STOP_WORDS
# from .tokenizer_exceptions import TOKENIZER_EXCEPTIONS
# from .lex_attrs import LEX_ATTRS

# from ..tokenizer_exceptions import BASE_EXCEPTIONS
# from ...language import Language
# from ...attrs import LANG
# from ...util import update_exc

# # create Defaults class in the module scope (necessary for pickling!)
# class XxxxxDefaults(Language.Defaults):
#     lex_attr_getters = dict(Language.Defaults.lex_attr_getters)
#     lex_attr_getters[LANG] = lambda text: 'xx' # language ISO code

#     # optional: replace flags with custom functions, e.g. like_num()
#     lex_attr_getters.update(LEX_ATTRS)

#     # merge base exceptions and custom tokenizer exceptions
#     tokenizer_exceptions = update_exc(BASE_EXCEPTIONS, TOKENIZER_EXCEPTIONS)
#     stop_words = STOP_WORDS

# # create actual Language class
# class Xxxxx(Language):
#     lang = 'xx' # language ISO code
#     Defaults = XxxxxDefaults # override defaults

# # set default export – this allows the language class to be lazy-loaded
# __all__ = ['Xxxxx']