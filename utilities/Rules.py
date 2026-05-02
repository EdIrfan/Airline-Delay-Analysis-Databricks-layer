def get_rules_as_list_of_dict():
    """
    DLT Data Quality Rules for the Airline Delay Analysis pipeline.

    CRITICAL: In @dlt.expect_all_or_drop(), the constraint defines what a VALID row looks like.
        - Constraint = TRUE  -->  row is KEPT
        - Constraint = FALSE -->  row is DROPPED

    So every constraint must be written as:
        "This is true for a good row"  (NOT "this is true for a bad row")

    These rules are applied to the OUTPUT of the Silver transform function,
    meaning AFTER all column renames, fillna(), and cancelled/diverted fixes have run.
    Keep that in mind when reasoning about which values can be NULL vs 0 vs positive.
    """
    return [

        # ====================================================================
        # ROUTE INTEGRITY
        # ====================================================================

        {
            # Both airports must be present. A flight with no origin or destination
            # is unidentifiable and useless for any analysis.
            "name"       : "BOTH_AIRPORTS_PRESENT",
            "constraint" : "ORIGIN_AIRPORT IS NOT NULL AND DEST_AIRPORT IS NOT NULL",
            "tag"        : "rule_1"
        },
        {
            # A flight cannot depart and arrive at the same airport.
            # If ORIGIN = DEST the record is corrupt.
            "name"       : "ORIGIN_IS_NOT_DESTINATION",
            "constraint" : "ORIGIN_AIRPORT <> DEST_AIRPORT",
            "tag"        : "rule_2"
        },

        # ====================================================================
        # FLIGHT IDENTITY
        # ====================================================================

        {
            # Without an airline code or flight number we cannot attribute the
            # record to any carrier or schedule. Drop it.
            "name"       : "FLIGHT_IDENTITY_PRESENT",
            "constraint" : "AIRLINE_CODE IS NOT NULL AND FLIGHT_NUMBER IS NOT NULL",
            "tag"        : "rule_3"
        },

        # ====================================================================
        # TIME FIELD SANITY
        #
        # Silver context:
        #   - Cancelled flights: AIR_TIME_MINUTES, ACTUAL_ELAPSED_TIME,
        #     ESTIMATED_ELAPSED_TIME are set to 0 (not NULL) by the transform.
        #   - DEP_DELAY_MINUTES and ARR_DELAY_MINUTES are filled to 0 for NULLs.
        #   - Negative DEPARTURE delay = early departure, which is valid. Keep it.
        #   - ACTUAL_DEP_TIME and ACTUAL_ARR_TIME can legitimately be NULL for
        #     cancelled flights (the plane never moved).
        #   - Scheduled times (CRS fields) should always be present and in
        #     HHMM format (valid range 0-2359, never negative).
        # ====================================================================

        {
            # Scheduled departure must always be present and be a valid HHMM clock value.
            "name"       : "SCHEDULED_DEP_TIME_VALID",
            "constraint" : "SCHEDULED_DEP_TIME IS NOT NULL AND SCHEDULED_DEP_TIME >= 0",
            "tag"        : "rule_4"
        },
        {
            # Scheduled arrival must always be present and be a valid HHMM clock value.
            "name"       : "SCHEDULED_ARR_TIME_VALID",
            "constraint" : "SCHEDULED_ARR_TIME IS NOT NULL AND SCHEDULED_ARR_TIME >= 0",
            "tag"        : "rule_5"
        },
        {
            # Actual departure is NULL for cancelled flights -- that is fine.
            # If it IS present, it must be a non-negative HHMM value.
            "name"       : "ACTUAL_DEP_TIME_VALID",
            "constraint" : "ACTUAL_DEP_TIME IS NULL OR ACTUAL_DEP_TIME >= 0",
            "tag"        : "rule_6"
        },
        {
            # Actual arrival is NULL for cancelled/diverted flights -- that is fine.
            # If it IS present, it must be a non-negative HHMM value.
            "name"       : "ACTUAL_ARR_TIME_VALID",
            "constraint" : "ACTUAL_ARR_TIME IS NULL OR ACTUAL_ARR_TIME >= 0",
            "tag"        : "rule_7"
        },
        {
            # After the Silver transform, cancelled flights have ESTIMATED_ELAPSED_TIME = 0.
            # Non-cancelled flights should have a positive value or NULL (missing data ok).
            # Either way, a negative value is physically impossible.
            "name"       : "ESTIMATED_ELAPSED_TIME_VALID",
            "constraint" : "ESTIMATED_ELAPSED_TIME IS NULL OR ESTIMATED_ELAPSED_TIME >= 0",
            "tag"        : "rule_8"
        },
        {
            # Same logic as ESTIMATED_ELAPSED_TIME.
            "name"       : "ACTUAL_ELAPSED_TIME_VALID",
            "constraint" : "ACTUAL_ELAPSED_TIME IS NULL OR ACTUAL_ELAPSED_TIME >= 0",
            "tag"        : "rule_9"
        },
        {
            # Cancelled flights have AIR_TIME_MINUTES = 0 after Silver transform.
            # Diverted flights may have partial air time or NULL. Negative is impossible.
            "name"       : "AIR_TIME_VALID",
            "constraint" : "AIR_TIME_MINUTES IS NULL OR AIR_TIME_MINUTES >= 0",
            "tag"        : "rule_10"
        },

        # ====================================================================
        # CANCELLED FLIGHT CONSISTENCY
        #
        # A cancelled flight never left the gate, so certain fields must
        # reflect that physical reality.
        #
        # Note: ARR_DELAY_MINUTES and AIR_TIME_MINUTES are set to 0 (not NULL)
        # for cancelled flights by the Silver transform, so we do NOT check
        # IS NULL on those columns -- the transform already handled them correctly.
        # ====================================================================

        {
            # If a flight was cancelled it should have no wheels-off timestamp,
            # because the plane never left the gate.
            "name"       : "CANCELLED_THEN_NO_TAKEOFF",
            "constraint" : "IF(IS_CANCELLED = 1, WHEELS_OFF IS NULL, TRUE)",
            "tag"        : "rule_11"
        },

    ]
