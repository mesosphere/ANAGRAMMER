import json

class Result(object):
	def __repr__(self):
		return json.dumps(self.__dict__, sort_keys = True)

class CrawlResult(Result):
	"""The result of mining a word for anagrams

	CrawlResult must serialize to JSON as its default representation:

	>>> res = CrawlResult(
	... 	"1234",
	... 	"foo",
	... 	["foo", "oof"]
	... )
	>>> repr(res)
	'{"anagrams": ["foo", "oof"], "t2skId": "1234", "word": "foo"}'
	"""
	def __init__(self, taskId, word, anagrams):
		self.taskId = taskId
		self.word   = word
		self.anagrams  = anagrams

class RenderResult(Result):
	"""The result of getting the definition of an anagram

	RenderResult must serialize to JSON as its default representation:

	>>> res = RenderResult(
	... 	"1234",
	... 	"foo",
	... 	"definition of foo"
	... )
	>>> repr(res)
	'{"definition": "definition of foo", "taskId": "1234", "word": "foo"}'
	"""
	def __init__(self, taskId, word, definition):
		self.taskId   = taskId
		self.word     = word
		self.definition = definition 

if __name__ == "__main__":
    import doctest
    doctest.testmod()
