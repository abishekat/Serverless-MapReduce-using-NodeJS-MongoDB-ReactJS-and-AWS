const { MongoClient } = require("mongodb");

const test1 = async () => {
  const uri =
    "mongodb+srv://Abishek:1234@test.frpr17t.mongodb.net/?retryWrites=true&w=majority";
  const client = new MongoClient(uri);

  try {
    await client.connect();
    await mapReduce(client);
  } catch (e) {
    console.error(e);
  } finally {
    await client.close();
  }
};
test1().catch(console.error);

const mapReduce = async (client) => {
  const db = await client.db("POC_ASSIGNMENT_2");

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

  const batchUnit = 500;
  const batchSize = 1;
  const batchId = 1;
  const max = [
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
      $match: {
        CPUUtilization_Average: "58",
      },
    },
    {
      $group: {
        _id: "$CPUUtilization_Average",
        NetworkOut_Average: {
          $max: "$convertedNetworkOut_Average",
        },
      },
    },
  ];
  const min = [
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
      $match: {
        CPUUtilization_Average: "58",
      },
    },
    {
      $group: {
        _id: "$CPUUtilization_Average",
        NetworkOut_Average: {
          $min: "$convertedNetworkOut_Average",
        },
      },
    },
  ];
  const average = [
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
      $match: {
        CPUUtilization_Average: "58",
      },
    },
    {
      $group: {
        _id: "$CPUUtilization_Average",
        NetworkOut_Average: {
          $avg: "$convertedNetworkOut_Average",
        },
      },
    },
  ];
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

  const maximum = await db.collection("SlicedData").aggregate(max).toArray();
  const minimum = await db.collection("SlicedData").aggregate(min).toArray();
  const avg = await db.collection("SlicedData").aggregate(average).toArray();
  console.log("resp :::" + JSON.stringify(resp));
  console.log("response :::" + JSON.stringify(response));
  console.log("maximum ::: " + JSON.stringify(maximum));
  console.log("minimum ::: " + JSON.stringify(minimum));
  console.log("average ::: " + JSON.stringify(avg));

  const output = {
    statusCode: 200,
    body: "MapReduce Sucessfull",
  };
  return output;
};
