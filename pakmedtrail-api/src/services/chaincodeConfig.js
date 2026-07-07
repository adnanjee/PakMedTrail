// Verified against Go source at:
// /home/adnan/go/src/github.com/adnanjee/PakMedTrail/chaincode

const CHAINCODES = {
  apitransfer: {
    name: 'apitransfer',
    functions: {
      // Lot lifecycle
      createLot: 'CreateLot',
      readLot: 'ReadLot',
      lotExists: 'LotExists',
      deleteLot: 'DeleteLot',
      updateMetadata: 'UpdateMetadata',
      consume: 'Consume',
      destroy: 'Destroy',
      // DRAP regulatory
      approveLotByDRAP: 'ApproveLotByDRAP',
      rejectLotByDRAP: 'RejectLotByDRAP',
      // Transfer workflow
      proposeTransfer: 'ProposeTransfer',
      acceptTransfer: 'AcceptTransfer',
      rejectTransfer: 'RejectTransfer',
      cancelTransfer: 'CancelTransfer',
      expireTransfer: 'ExpireTransfer',
      // PDC
      readSensitive: 'ReadSensitive',
      linkSensitiveHash: 'LinkSensitiveHash',
      // Queries
      getAllLots: 'GetAllLots',
      getLotsByOwner: 'GetLotsByOwner',
      getLotsPendingDRAPApproval: 'GetLotsPendingDRAPApproval',
      queryLots: 'QueryLots',
      queryLotsBy: 'QueryLotsBy',
      getHistory: 'GetHistory'
    }
  },
  manufacturing: {
    name: 'manufacturing',
    functions: {
      // Formulation
      createFormulation: 'CreateFormulation',
      readFormulation: 'ReadFormulation',
      updateFormulation: 'UpdateFormulation',
      // Batch production
      produceDrug: 'ProduceDrug',
      readBatch: 'ReadBatch',
      batchExists: 'BatchExists',
      destroyBatch: 'DestroyBatch',
      resetBatchToStock: 'ResetBatchToStock',
      updateBatchMetadata: 'UpdateBatchMetadata',
      // DRAP regulatory
      approveDrugBatchByDRAP: 'ApproveDrugBatchByDRAP',
      rejectDrugBatchByDRAP: 'RejectDrugBatchByDRAP',
      // Transfer
      proposeBatchTransfer: 'ProposeBatchTransfer',
      acceptBatchTransfer: 'AcceptBatchTransfer',
      rejectBatchTransfer: 'RejectBatchTransfer',
      cancelBatchTransfer: 'CancelBatchTransfer',
      expireBatchTransfer: 'ExpireBatchTransfer',
      // Queries
      getAllFormulations: 'GetAllFormulations',
      getBatchesByOwner: 'GetBatchesByOwner',
      getBatchesByProducer: 'GetBatchesByProducer',
      getBatchesByStatus: 'GetBatchesByStatus',
      getBatchesPendingDRAPApproval: 'GetBatchesPendingDRAPApproval',
      getBatchesPendingTransfer: 'GetBatchesPendingTransfer',
      queryBatches: 'QueryBatches',
      getBatchHistory: 'GetBatchHistory'
    }
  },
  distribution: {
    name: 'distribution',
    functions: {
      // Shipment lifecycle
      createShipmentOffer: 'CreateShipmentOffer',
      acceptShipment: 'AcceptShipment',
      rejectShipment: 'RejectShipment',
      cancelShipment: 'CancelShipment',
      markDelivered: 'MarkDelivered',
      readShipment: 'ReadShipment',
      // PDC
      readSensitive: 'ReadSensitive',
      linkSensitiveHash: 'LinkSensitiveHash',
      // Recall integration
      quarantineByRecall: 'QuarantineByRecall',
      // Queries
      getShipmentsByParty: 'GetShipmentsByParty',
      queryShipments: 'QueryShipments',
      queryRecalls: 'QueryRecalls'
    }
  },
  retail: {
    name: 'retail',
    functions: {
      // Shipment lifecycle
      createRetailShipmentOffer: 'CreateRetailShipmentOffer',
      acceptRetailShipment: 'AcceptRetailShipment',
      rejectRetailShipment: 'RejectRetailShipment',
      cancelRetailShipment: 'CancelRetailShipment',
      markRetailDelivered: 'MarkRetailDelivered',
      readShipment: 'ReadShipment',
      shipmentExists: 'ShipmentExists',
      updateShipmentMetadata: 'UpdateShipmentMetadata',
      // Dispense
      verifyDispense: 'VerifyDispense',
      readDispense: 'ReadDispense',
      queryDispenses: 'QueryDispenses',
      // PDC
      readSensitive: 'ReadSensitive',
      linkSensitiveHash: 'LinkSensitiveHash',
      // Recall integration
      quarantineByRecall: 'QuarantineByRecall',
      readRecall: 'ReadRecall',
      // Queries
      getShipmentsByStatus: 'GetShipmentsByStatus',
      getShipmentsByParty: 'GetShipmentsByParty',
      getActiveRecalls: 'GetActiveRecalls',
      getShipmentHistory: 'GetShipmentHistory',
      queryShipments: 'QueryShipments',
      queryRecalls: 'QueryRecalls'
    }
  },
  recall: {
    name: 'recall',
    functions: {
      // DRAP lifecycle
      initiateRecallByDRAP: 'InitiateRecallByDRAP',
      closeRecallByDRAP: 'CloseRecallByDRAP',
      addAffectedAssetsByDRAP: 'AddAffectedAssetsByDRAP',
      // Stakeholder actions
      acknowledgeRecall: 'AcknowledgeRecall',
      quarantineAsset: 'QuarantineAsset',
      clearQuarantine: 'ClearQuarantine',
      // Queries
      readRecall: 'ReadRecall',
      isAssetUnderActiveRecall: 'IsAssetUnderActiveRecall',
      listActiveRecalls: 'ListActiveRecalls',
      getQuarantine: 'GetQuarantine',
      listAffectsByAsset: 'ListAffectsByAsset'
    }
  }
}

module.exports = { CHAINCODES }
