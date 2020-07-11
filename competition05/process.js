// Title: [Open] Shopee Code League - Logistics (Logistics Performance)
// Team: Jinjcha?!
// Author: Jin

// const INPUT_FILE = "delivery_orders_march_sample.csv";
const INPUT_FILE = "delivery_orders_march.csv";

const moment = require("moment");
const csv = require("csv-parser");
const fs = require("fs");

const errors = [];

const publicHolidays = [
	"2020-03-08",
	"2020-03-25",
	"2020-03-30",
	"2020-03-31",
].map((item) => {
	return moment(item, "YYYY-MM-DD");
});
const addWorkingDays = (date, days) => {
	// not ultra efficient but good enough for competition

	date = moment(date); // use a clone
	while (days > 0) {
		date = date.add(1, "days");

		// check if new date is a publicHoliday
		const isPublicHoliday = publicHolidays.some((item) => date.isSame(item));

		// decrease "days" only if it's a sunday and not a public holiday.
		if (!isPublicHoliday && date.isoWeekday() !== 7) {
			days -= 1;
		}
	}
	return date;
};

// sla - seller_to_buyer
const sla = {
	"metro manila": {
		"metro manila": 3,
		luzon: 5,
		visayas: 7,
		mindanao: 7,
	},
	luzon: {
		"metro manila": 5,
		luzon: 5,
		visayas: 7,
		mindanao: 7,
	},
	visayas: {
		"metro manila": 7,
		luzon: 7,
		visayas: 7,
		mindanao: 7,
	},
	mindanao: {
		"metro manila": 7,
		luzon: 7,
		visayas: 7,
		mindanao: 7,
	},
};
const zones = Object.keys(sla).map((item) => ` ${item} `);

const cleanAddress = (item) => {
	const cleaned = String(item)
		.toLowerCase()
		.replace(/[\W\d_]/g, " ") // convert any non-characters to a space
		.replace(/\s\s+/g, " ") // remove extra spaces
		.trim();

	return ` ${cleaned} `;
};
const getZone = (address) => {
	let result = false;
	let foundZones = 0;
	let lastfoundPosition = -1;

	for (var i = 0; i < zones.length; i++) {
		const currentZone = zones[i];

		const foundPosition = address.indexOf(currentZone);
		// in case the address matches with 2 zones, we will prioritise zones found later in the address
		if (foundPosition > lastfoundPosition) {
			result = currentZone.trim();
			lastfoundPosition = foundPosition;
			foundZones += 1;
			// break;
		}
	}
	if (foundZones > 1) errors.push(address);
	return result;
};
const getSla = (seller, buyer) => {
	const seller_clean = cleanAddress(seller);
	const buyer_clean = cleanAddress(buyer);

	const seller_zone = getZone(seller_clean);
	const buyer_zone = getZone(buyer_clean);

	const noError = !!seller_zone && !!buyer_zone;

	return noError ? sla[seller_zone][buyer_zone] : false;
};

const processData = (data) => {
	const {
		orderid,
		pick,
		first_deliver_attempt,
		second_deliver_attempt,
		buyeraddress,
		selleraddress,
	} = data;

	const hasSecondAttempt = second_deliver_attempt !== "";

	const pick_moment = moment.unix(pick).startOf("day");
	const first_deliver_moment = moment
		.unix(first_deliver_attempt)
		.startOf("day");
	const second_deliver_moment = hasSecondAttempt
		? moment.unix(second_deliver_attempt).startOf("day")
		: false;

	const sla_days = getSla(selleraddress, buyeraddress);

	if (sla_days === false) {
		errors.push(data);
	} else {
		// the following can be abstracted out to a function, but im lazy
		const sla_checks = [];

		const first_sla = addWorkingDays(pick_moment, sla_days);
		const first_sla_pass = first_deliver_moment.isSameOrBefore(first_sla);
		let is_late = first_sla_pass === false;

		if (!is_late && hasSecondAttempt) {
			const second_sla = addWorkingDays(first_deliver_moment, 3);
			const second_sla_pass = second_deliver_moment.isSameOrBefore(second_sla);
			is_late = second_sla_pass === false;
		}

		return [orderid, is_late | 0]; // we use a trick here to convert boolean to digit - https://stackoverflow.com/a/22239859
	}
};

let output_csv = fs.openSync("output.csv", "w");

let skippedFirstLine = false;
fs.createReadStream(INPUT_FILE)
	.pipe(
		csv({
			// define headers, because i seem to have problem referencing variables with the headers given via CSV files
			headers: [
				"orderid",
				"pick",
				"first_deliver_attempt",
				"second_deliver_attempt",
				"buyeraddress",
				"selleraddress",
			],
		})
	)
	.on("data", (data) => {
		// which requires me to "hack" and skip the first line
		if (skippedFirstLine === false) {
			skippedFirstLine = true;
			fs.writeSync(output_csv, `orderid,is_late\n`);
			return;
		}

		const results = processData(data);
		fs.writeSync(output_csv, `${results[0]},${results[1]}\n`);
	})
	.on("end", () => {
		console.log(errors);
	});
