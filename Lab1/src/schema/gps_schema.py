from marshmallow import Schema, fields
from domain.gps import Gps

class GpsSchema(Schema):
    latitude = fields.Number()
    longitude = fields.Number()