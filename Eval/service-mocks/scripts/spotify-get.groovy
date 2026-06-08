// Dispatcher per tutte le operazioni GET di Spotify.
// Per la maggior parte degli endpoint esiste un solo esempio 'mock',
// quindi ritorniamo sempre 'mock'.
//
// Eccezione: GET /search — il tipo di risposta dipende dal query param 'type':
//   type=album  → 'mock_album'   {"albums":  {"items": [{id, name, release_date, artists}]}}
//   type=artist → 'mock_artist'  {"artists": {"items": [{id, name}]}}
//   type=track  → 'mock'         {"tracks":  {"items": [{id, name, artists}]}}  (default)
//
// Il routing per operazione avviene prima a livello Microcks (path-based),
// poi questo script seleziona l'esempio. Per tutti gli endpoint diversi
// da /search il controllo sul param 'type' è innocuo (non esiste → fallback 'mock').

def params = mockRequest.getQueryParameters()
def type   = params?.get('type')?.toString()?.toLowerCase() ?: ''

if (type == 'album')  return 'mock_album'
if (type == 'artist') return 'mock_artist'
return 'mock'