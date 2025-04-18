# AIS Data Dictionary

The following table describes the columns in the AIS (Automatic Identification System) data files:

| # | Name | Description | Example | Units | Resolution | Type | Size |
|---|------|-------------|---------|-------|------------|------|------|
| 1 | MMSI | Maritime Mobile Service Identity value | 477220100 | | | Text | 9 |
| 2 | BaseDateTime | Full UTC date and time | 2017-02-01T20:05:07 | | YYYY-MM-DD:HH-MM-SS | DateTime | |
| 3 | LAT | Latitude | 42.35137 | decimal degrees | XX.XXXXX | Double | 8 |
| 4 | LON | Longitude | -71.04182 | decimal degrees | XXX.XXXXX | Double | 8 |
| 5 | SOG | Speed Over Ground | 5.9 | knots | XXX.X | Float | 4 |
| 6 | COG | Course Over Ground | 47.5 | degrees | XXX.X | Float | 4 |
| 7 | Heading | True heading angle | 45.1 | degrees | XXX.X | Float | 4 |
| 8 | VesselName | Name as shown on the station radio license | OOCL Malaysia | | | Text | 32 |
| 9 | IMO | International Maritime Organization Vessel number | IMO9627980 | | | Text | 7 |
| 10 | CallSign | Call sign as assigned by FCC | VRME7 | | | Text | 8 |
| 11 | VesselType | Vessel type as defined in NAIS specifications | 70 | | | Integer | short |
| 12 | Status | Navigation status as defined by the COLREGS | 3 | | | Integer | short |
| 13 | Length | Length of vessel (see NAIS specifications) | 71.0 | meters | XXX.X | Float | 4 |
| 14 | Width | Width of vessel (see NAIS specifications) | 12.0 | meters | XXX.X | Float | 4 |
| 15 | Draft | Draft depth of vessel (see NAIS specifications) | 3.5 | meters | XXX.X | Float | 4 |
| 16 | Cargo | Cargo type (see NAIS specification and codes) | 70 | | | Text | 4 |
| 17 | TransceiverClass | Class of AIS transceiver | A | | | Text | 2 |

## Special Notes:

1. **Heading Value of 511.0**: Indicates that heading information is not available
2. **VesselType Value of 0**: Often means "not available/undefined"
3. **DateTime Format**: Timestamps are in ISO format with 'T' separator (YYYY-MM-DDThh:mm:ss)
4. **Missing Values**: Many fields may contain empty strings or NULL values
5. **TransceiverClass**: Usually 'A' or 'B', indicating different types of AIS transponders
   - Class A: Required on large commercial vessels
   - Class B: Used by smaller commercial vessels and recreational boats

## Navigation Status Codes:

| Status Code | Description |
|-------------|-------------|
| 0 | Under way using engine |
| 1 | At anchor |
| 2 | Not under command |
| 3 | Restricted maneuverability |
| 4 | Constrained by her draft |
| 5 | Moored |
| 6 | Aground |
| 7 | Engaged in fishing |
| 8 | Under way sailing |
| 9 | Reserved for future amendment (HSC) |
| 10 | Reserved for future amendment (WIG) |
| 11 | Power-driven vessel towing astern |
| 12 | Power-driven vessel pushing ahead/alongside |
| 13 | Reserved for future use |
| 14 | AIS-SART, MOB-AIS, EPIRB-AIS |
| 15 | Undefined |

Source: MarineCadastre.gov
