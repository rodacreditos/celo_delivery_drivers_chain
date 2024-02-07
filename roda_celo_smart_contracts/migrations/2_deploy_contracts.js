const fs = require('fs');
const RodaRoute = artifacts.require("RodaRoute");
const RodaCreditCOP = artifacts.require("RodaCreditCOP");

module.exports = function (deployer) {
  const contractName = process.env.SMART_CONTRACT_NAME;
  switch (contractName) {
    case 'RodaRoute':
      console.log('Deploying routes contract');
      deployer.deploy(RodaRoute).then(() => {
        let config = {
          RODA_ROUTE_CONTRACT_ADDR: RodaRoute.address,
          RODA_ROUTE_CONTRACT_ABI: RodaRoute.abi
        };
        fs.writeFileSync('./credentials/roda_celo_contracts.json', JSON.stringify(config, null, 2));
      });
      break;
    case 'RodaCreditCOP':
      console.log('Deploying credits contract');
      deployer.deploy(RodaCreditCOP).then(() => {
        let config = {
          RODA_CREDIT_CONTRACT_ADDR: RodaCreditCOP.address,
          RODA_CREDIT_CONTRACT_ABI: RodaCreditCOP.abi
        };
        fs.writeFileSync('./credentials/roda_credits_contract.json', JSON.stringify(config, null, 2));
      });
      break;
    default:
      console.log(`${contractName} is not known and we cannot publish any`);
  }
};