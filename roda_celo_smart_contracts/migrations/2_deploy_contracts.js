const fs = require('fs');
const RodaRoute = artifacts.require("RodaRoute");

module.exports = function (deployer) {
  deployer.deploy(RodaRoute).then(() => {
    let config = {
      RODA_ROUTE_CONTRACT_ADDR: RodaRoute.address,
    };
    fs.writeFileSync('./credentials/roda_celo_contracts.json', JSON.stringify(config));
  });
};