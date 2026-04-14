def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def status = q("status")
if (status != null) status = status.toLowerCase()
if (status == "open")        return "open"
if (status == "resolved")    return "resolved"
if (status == "in_progress") return "in_progress"
if (status == "closed")      return "closed"

def category = q("category")
if (category != null) category = category.toLowerCase()
if (category == "sanitation")     return "sanitation"
if (category == "infrastructure") return "infrastructure"
if (category == "safety")         return "safety"
if (category == "lighting")       return "lighting"
if (category == "traffic")        return "traffic"
if (category == "environment")    return "environment"
if (category == "other")          return "other"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
