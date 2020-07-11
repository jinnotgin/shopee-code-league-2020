// external dependencies
const neatCsv = require("neat-csv");
const moment = require("moment");

// internal dependencies
const fs = require("fs");

// begin
fs.readFile("./order_brush_order.csv", async (err, data) => {
	if (err) {
		console.error(err);
		return;
	}

	// process the data
	const unique_shopsids = [];
	const raw_jsonData = await neatCsv(data);
	const jsonData = raw_jsonData
		.map((transaction) => {
			const { orderid, shopid, userid, event_time } = transaction;
			const event_moment = moment(event_time);
			if (!unique_shopsids.includes(shopid)) unique_shopsids.push(shopid);

			return {
				orderid,
				shopid,
				userid,
				event_time,
				event_moment,
			};
		})
		.sort((a, b) => {
			return a.event_moment - b.event_moment;
		});
	fs.writeFile("sorted.json", JSON.stringify(jsonData), (err) => {
		if (err) throw err;
		console.log("Data written to file");
	});

	const brushingPeriods = {
		// define the data structure
		// shopId: [{ start: MOMENT, before: MOMENT, ordersPerUser }],
	};

	jsonData.map((transaction, index) => {
		// debug
		// if (index > 10000) return false;

		const { orderid, shopid, userid, event_time, event_moment } = transaction;
		const afterOneHour_moment = event_moment.clone().add(1, "h");

		const context = {
			shopid,
			event_moment,
			afterOneHour_moment,
		};
		// console.log(context);

		const ordersPerUser = {
			// userId: [orderId]
		};

		for (j = index + 1; j < jsonData.length; j++) {
			const nextTransaction = jsonData[j];
			const { shopid, userid, orderid, event_moment } = nextTransaction;

			const _isWithinTime =
				event_moment.isSameOrAfter(context.event_moment) &&
				event_moment.isBefore(context.afterOneHour_moment);
			if (!_isWithinTime) break;

			const _isSameShop = shopid === context.shopid;
			if (_isSameShop) {
				if (typeof ordersPerUser[userid] === "undefined") {
					ordersPerUser[userid] = [];
				}
				ordersPerUser[userid].push(orderid);
			}
		}

		const uniqueBuyers = Object.keys(ordersPerUser).length;
		const countOfOrders = Object.values(ordersPerUser).reduce(
			(total, current) => total + current.length,
			0
		);
		const concentrateRate =
			countOfOrders === 0 ? -1 : countOfOrders / uniqueBuyers;
		// console.log(concentrateRate);

		if (concentrateRate >= 3) {
			console.log("found another one");
			console.log(concentrateRate);
			console.log(context);
			console.log(ordersPerUser);

			const { shopid, event_moment, afterOneHour_moment } = context;

			if (typeof brushingPeriods[shopid] === "undefined") {
				brushingPeriods[shopid] = [];
			}
			brushingPeriods[shopid].push({
				start: event_moment,
				before: afterOneHour_moment,
				ordersPerUser,
			});
		}
	});

	// console.log(brushingPeriods);
	fs.writeFile(
		"brushing_periods.json",
		JSON.stringify(brushingPeriods),
		(err) => {
			if (err) throw err;
			console.log("Data written to file");
		}
	);

	// combine all brushing perods per shop
	const perShopAcrossPeriods = {};
	Object.entries(brushingPeriods).map(([shopid, shopBrushingPeriods]) => {
		shopBrushingPeriods.map((period) => {
			const { ordersPerUser } = period;
			if (typeof perShopAcrossPeriods[shopid] === "undefined") {
				perShopAcrossPeriods[shopid] = {};
			}

			Object.entries(ordersPerUser).map(([userid, orders]) => {
				if (typeof perShopAcrossPeriods[shopid][userid] === "undefined") {
					perShopAcrossPeriods[shopid][userid] = {
						count: 0,
						orders: [],
					};
				}

				orders.map((orderId) => {
					const _orderIdDoesNotExist = !perShopAcrossPeriods[shopid][
						userid
					].orders.includes(orderId);
					if (_orderIdDoesNotExist) {
						perShopAcrossPeriods[shopid][userid].orders.push(orderId);
						perShopAcrossPeriods[shopid][userid].count += 1;
					}
				});
			});
		});
	});
	// console.log(perShopAcrossPeriods);
	fs.writeFile(
		"perShopAcrossPeriods.json",
		JSON.stringify(perShopAcrossPeriods),
		(err) => {
			if (err) throw err;
			console.log("Data written to file");
		}
	);

	// for per shop, find out what is the highest count orders contributed by user (highest Order Proportion)
	const maxOrdersPerUserPerShop = {};
	Object.entries(perShopAcrossPeriods).map(([shopid, ordersPerUser]) => {
		if (typeof maxOrdersPerUserPerShop[shopid] === "undefined") {
			maxOrdersPerUserPerShop[shopid] = 0;
		}
		Object.entries(ordersPerUser).map(([userid, orders]) => {
			const { count } = orders;
			if (count > maxOrdersPerUserPerShop[shopid])
				maxOrdersPerUserPerShop[shopid] = count;
		});
	});
	console.log(maxOrdersPerUserPerShop);
	fs.writeFile(
		"maxOrdersPerUserPerShop.json",
		JSON.stringify(maxOrdersPerUserPerShop),
		(err) => {
			if (err) throw err;
			console.log("Data written to file");
		}
	);

	// for all shops, list out users who have the highest Order Proportion
	const maxContributingUsersPerShop = {};
	unique_shopsids.map(
		(shopid) => (maxContributingUsersPerShop[shopid] = ["0"])
	);
	Object.entries(perShopAcrossPeriods).map(([shopid, ordersPerUser]) => {
		Object.entries(ordersPerUser).map(([userid, orders]) => {
			const { count } = orders;
			if (count === maxOrdersPerUserPerShop[shopid]) {
				if (maxContributingUsersPerShop[shopid][0] === "0") {
					maxContributingUsersPerShop[shopid].shift();
				}
				maxContributingUsersPerShop[shopid].push(userid);
			}
		});
	});
	console.log(maxContributingUsersPerShop);
	fs.writeFile(
		"maxContributingUsersPerShop.json",
		JSON.stringify(maxContributingUsersPerShop),
		(err) => {
			if (err) throw err;
			console.log("Data written to file");
		}
	);

	// write out the results into CSV format
	let output_csv = "shopid,userid\n";
	Object.entries(maxContributingUsersPerShop).map(([shopid, userid_list]) => {
		output_csv += `${shopid},${userid_list
			.sort((a, b) => parseInt(a) - parseInt(b))
			.join("&")}\n`;
	});
	console.log(output_csv);
	fs.writeFile("output.csv", output_csv, (err) => {
		if (err) throw err;
		console.log("Data written to file");
	});
});
