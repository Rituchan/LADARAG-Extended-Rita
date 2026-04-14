def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def status = q("status")
if (status != null) status = status.toLowerCase()
if (status == "open")              return "open"
if (status == "temporarily_closed") return "temporarily_closed"
if (status == "closed")            return "closed"
if (status == "special_event")     return "special_event"

def wheelchairAccessible = q("wheelchairAccessible")
if (wheelchairAccessible == "true") return "accessible_only"

def category = q("category")
if (category != null) category = category.toLowerCase()
if (category == "archaeological") return "archaeological"
if (category == "museum")         return "museum"
if (category == "church")         return "church"
if (category == "palace")         return "palace"
if (category == "park")           return "park"
if (category == "monument")       return "monument"
if (category == "theatre")        return "theatre"
if (category == "religious")      return "religious"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
