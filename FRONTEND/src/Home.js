import React, { useState } from "react";
import axios from "axios";
import "./App.css";

const loginUrl =
  "https://mmk8b66r7g.execute-api.us-east-1.amazonaws.com/dev/mapReduce";

const Home = () => {
  const [batchUnit, setbatchUnit] = useState("");
  const [batchSize, setbatchSize] = useState("");
  const [batchId, setbatchId] = useState("");
  const [reduced, setreduced] = useState("");
  const [sum, setsum] = useState("");

  const handleSubmit = (e) => {
    e.preventDefault();

    const requestConfig = {
      headers: {
        "content-type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    };

    const requestBody = {
      requestParams: {
        batchUnit: batchUnit,
        batchSize: batchSize,
        batchId: batchId,
      },
    };

    axios
      .post(loginUrl, requestBody, requestConfig)
      .then((response) => {
        console.log(response);
        const resp = JSON.parse(response);
        setreduced(resp[0].reduced);
        setsum(resp[0].total);
      })
      .catch((error) => {
        console.log(error);
      });
  };

  return (
    <>
      <div className="Auth-form-container">
        <form className="Auth-form" onSubmit={handleSubmit}>
          <div className="Auth-form-content">
            <div className="form-group mt-3">
              <label>Batch Unit</label>
              <input
                type="batchUnit"
                className="form-control mt-1"
                placeholder="batchUnit"
                value={batchUnit}
                onChange={(e) => setbatchUnit(e.target.value)}
              />
            </div>
            <div className="form-group mt-3">
              <label>Batch Size</label>
              <input
                type="batchSize"
                className="form-control mt-1"
                placeholder="batchSize"
                value={batchSize}
                onChange={(e) => setbatchSize(e.target.value)}
              />
            </div>
            <div className="form-group mt-3">
              <label>Batch ID</label>
              <input
                type="batchId"
                className="form-control mt-1"
                placeholder="batchId"
                value={batchId}
                onChange={(e) => setbatchId(e.target.value)}
              />
            </div>

            <div className="d-grid gap-2 mt-3">
              <button type="submit" className="primary-login-button">
                ENTER
              </button>
            </div>
            <div className="form-group mt-3">
              <label>No of Reduce Operation</label>
              <input
                type="batchUnit"
                className="form-control mt-1"
                placeholder="reduced"
                value={reduced}
              />
            </div>
            <div className="form-group mt-3">
              <label>Total</label>
              <input
                type="sum"
                className="form-control mt-1"
                placeholder="sum"
                value={sum}
              />
            </div>
          </div>
        </form>
      </div>
    </>
  );
};

export default Home;
