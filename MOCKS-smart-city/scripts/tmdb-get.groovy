// TMDB dispatcher — returns the named 'mock' example for every operation.
// Microcks routes the request to the correct operation (path-based), then
// this script selects which example to return. Since each operation has
// exactly one example named 'mock', we always return that name.
// Uses mockRequest (standard Microcks API) instead of the non-standard 'request'.

def req = mockRequest.getRequest()
return 'mock'
