// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol"; // Import SafeMath library

/**
 * @title RodaCreditCOP
 * @dev This contract represents a credit token with outstanding balance, issuance date, and credit term.
 */
contract RodaRoute is ERC721, AccessControl {
    using SafeMath for uint; // Use SafeMath for uint type

    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");

    // Creating a mapping to store the principal for each token Id.
    mapping(uint => uint) public timestampStart;
    // Creating a mapping to store the outstandingBalance for each token Id.
    mapping(uint => uint) public timestampEnd;
    // Creating a mapping to store the issuanceDate for each token Id.
    mapping(uint => uint) public distance;

    constructor() ERC721("RodaRoute", "RROUTE") {
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(MINTER_ROLE, msg.sender);
    }

    event RouteRecorded(
        address indexed to,
        uint256 indexed tokenId,
        uint256 timestampStart,
        uint256 timestampEnd,
        uint256 distance
    );

    function recordRoute(address to, uint routeId, uint _timestampStart, uint _timestampEnd, uint _distance) public onlyRole(MINTER_ROLE) {
        timestampStart[routeId] = _timestampStart;
        timestampEnd[routeId] = _timestampEnd;
        distance[routeId] = _distance;

        _safeMint(to, routeId); // Effects should be called last.

        emit RouteRecorded(to, routeId, _timestampStart, _timestampEnd, _distance);
    }

    function supportsInterface(bytes4 interfaceId)
        public
        view
        override(ERC721, AccessControl)
        returns (bool)
    {
        return super.supportsInterface(interfaceId);
    }
}