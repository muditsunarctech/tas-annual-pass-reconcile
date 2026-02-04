<?php

return [
    'plaza_constant' => 0.875,

    // Plaza ID => Single Side Fare (Example values, user must populate)
    'fares' => [
        // IDFC Plazas
        '142001' => 0.0,
        '142002' => 0.0,
        '220001' => 0.0,
        '220002' => 0.0,
        '220003' => 0.0,
        '220004' => 0.0,
        '235001' => 0.0,
        '235002' => 0.0,
        '243000' => 0.0,
        '243001' => 0.0,
        '073001' => 0.0,
        '073002' => 0.0,
        '073003' => 0.0,

        // ICICI Plazas
        '540030' => 0.0,
        '540032' => 0.0,
        '120001' => 0.0,
        '120002' => 0.0,
        '139001' => 0.0,
        '139002' => 0.0,
        '167001' => 0.0,
        '167002' => 0.0,
        '169001' => 0.0,
        '234002' => 0.0,
        '352001' => 0.0,
        '352013' => 0.0,
        '352065' => 0.0,
        '045001' => 0.0,
        '046001' => 0.0,
        '046002' => 0.0,
        '079001' => 0.0,
    ],

    'bank_plaza_map' => [
        "IDFC" => [
            "142001" => ["Ghoti", "IHPL"],
            "142002" => ["Arjunali", "IHPL"],
            "220001" => ["Raipur", "BPPTPL"],
            "220002" => ["Indranagar", "BPPTPL"],
            "220003" => ["Birami", "BPPTPL"],
            "220004" => ["Uthman", "BPPTPL"],
            "235001" => ["Mandawada", "SUTPL"],
            "235002" => ["Negadiya", "SUTPL"],
            "243000" => ["Rupakheda", "BRTPL"],
            "243001" => ["Mujras", "BRTPL"],
            "073001" => ["Bollapalli", "SEL"],
            "073002" => ["Tangutur", "SEL"],
            "073003" => ["Musunur", "SEL"]
        ],
        "ICICI" => [
            "540030" => ["Ladgaon", "CSJTPL"],
            "540032" => ["Nagewadi", "CSJTPL"],
            "120001" => ["Shanthigrama", "DHTPL"],
            "120002" => ["Kadabahalli", "DHTPL"],
            "139001" => ["Shirpur", "DPTL"],
            "139002" => ["Songir", "DPTL"],
            "167001" => ["Vaniyambadi", "KWTPL"],
            "167002" => ["Pallikonda", "KWTPL"],
            "169001" => ["Palayam", "KTTRL"],
            "234002" => ["Chagalamarri", "REPL"],
            "352001" => ["Nannur", "REPL"],
            "352013" => ["Chapirevula", "REPL"],
            "352065" => ["Patimeedapalli", "REPL"],
            "045001" => ["Gudur", "HYTPL"],
            "046001" => ["Kasaba", "BHTPL"],
            "046002" => ["Nagarhalla", "BHTPL"],
            "079001" => ["Shakapur", "WATL"]
        ]
    ],

    'plaza_id_headers' => ["PLAZA_ID", " Plaza ID", "Entry Plaza Code", "Entry Plaza Id", " Plaza Code", " Entry Plaza Code"],
    'annual_pass_values' => ["ANNUALPASS", "ANNUAL PASS"],

    'bank_column_map' => [
        "ICICI" => ["FastagReasonCode" => ["Reason", "Reason Code"]],
        "IDFC" => ["FastagReasonCode" => " Trc Vrc Reason Code"]
    ],

    'output_columns' => [
        "ICICI" => [
            "TransactionDateTime" => ["Transaction Date", "Entry Txn Date"],
            "VRN" => ["Licence Plate No.", "License Plate No."],
            "TagID" => ["Tag Id", "Hex Tag No"],
            "TripType" => ["Trip Type", "TRIPTYPEDISCRIPTION"]
        ],
        "IDFC" => [
            "TransactionDateTime" => ["READER_READ_TIME", " Reader Read Time"],
            "VRN" => ["VEHICLE_REG_NO", " Vehicle Reg. No."],
            "TagID" => ["TAG_ID", " Tag ID"],
            "TripType" => ["JOURNEY_TYPE", " Journey Type"]
        ]
    ]
];
