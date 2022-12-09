const MongoClient = require("mongodb").MongoClient;
const MONGODB_URI =
  "mongodb+srv://Abishek:1234@test.frpr17t.mongodb.net/?retryWrites=true&w=majority";
let cachedDb = null;

async function connectToDatabase() {
  if (cachedDb) {
    return cachedDb;
  }

  const client = await MongoClient.connect(MONGODB_URI);

  const db = await client.db("POC_ASSIGNMENT_2");

  cachedDb = db;
  return db;
}

module.exports.mapReduce = async (event, context) => {
  const db = await connectToDatabase();
  const file = JSON.parse(event.body);
  console.log("request :::" + JSON.stringify(file));

  const aggMap = [
    {
      $count: "totalMap",
    },
  ];

  const maxReduced = [
    {
      $sort: {
        reduced: -1,
      },
    },
    {
      $limit: 1,
    },
  ];

  const aggMapReduce = [
    {
      $addFields: {
        convertedCPUUtilization_Average: {
          $toInt: "$CPUUtilization_Average",
        },
        convertedNetworkIn_Average: {
          $toInt: "$NetworkIn_Average",
        },
        convertedNetworkOut_Average: {
          $toInt: "$NetworkOut_Average",
        },
        convertedMemoryUtilization_Average: {
          $toDouble: "$MemoryUtilization_Average",
        },
        convertedFinal_Target: {
          $toDouble: "$Final_Target",
        },
      },
    },
    {
      $group: {
        _id: "$CPUUtilization_Average",
        reduced: {
          $sum: 1,
        },
        total: {
          $sum: "$convertedNetworkIn_Average",
        },
      },
    },
  ];

  const batchUnit = file.requestParams.batchUnit
    ? file.requestParams.batchUnit
    : 10;
  const batchSize = file.requestParams.batchSize
    ? file.requestParams.batchSize
    : 1;
  const batchId = file.requestParams.batchId ? file.requestParams.batchId : 1;

  console.log(batchUnit + ":::" + batchSize + ":::" + batchId);
  const allItems = await db.collection("NDBench_Testing").find().toArray();
  const exists =
    (await (
      await db.listCollections().toArray()
    ).findIndex((item) => item.name === "SlicedData")) !== -1;

  const reducedExists =
    (await (
      await db.listCollections().toArray()
    ).findIndex((item) => item.name === "reduced")) !== -1;

  const startLimit = batchId * batchUnit;
  const endLimit = startLimit + batchSize * batchUnit;
  const resultData = allItems.slice(startLimit, endLimit);
  const totalBatches = allItems.length / batchUnit;
  if (exists) {
    await db.collection("SlicedData").drop();
  }
  await db.collection("SlicedData").insertMany(resultData);

  const reducedResult = await db
    .collection("SlicedData")
    .aggregate(aggMapReduce)
    .toArray();
  if (reducedExists) {
    await db.collection("reduced").drop();
  }
  await db.collection("reduced").insertMany(reducedResult);
  const mappedResult = await db
    .collection("reduced")
    .aggregate(aggMap)
    .toArray();

  const maxReducedFromArray = await db
    .collection("reduced")
    .aggregate(maxReduced)
    .toArray();

  context.callbackWaitsForEmptyEventLoop = false;
  const resp = {
    statusCode: 200,
    body: {
      reduced: reducedResult,
      mapped: JSON.stringify(mappedResult[0].totalMap),
    },
  };

  const response = {
    statusCode: 200,
    body: [
      {
        maxReduced: JSON.stringify(maxReducedFromArray[0].reduced),
        mapped: JSON.stringify(mappedResult[0].totalMap),
      },
    ],
  };

  console.log("resp :::" + JSON.stringify(resp));
  console.log("response :::" + JSON.stringify(response));

  const output = {
    statusCode: 200,
    body: "MapReduce Sucessfull",
  };
  return output;
};
