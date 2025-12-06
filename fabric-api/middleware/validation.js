const Joi = require('joi');

const lotCreateSchema = Joi.object({
  lotID: Joi.string().required(),
  name: Joi.string().required(),
  batchNumber: Joi.string().required(),
  quantity: Joi.number().integer().positive().required(),
  unit: Joi.string().required(),
  manufactureDate: Joi.string().pattern(/^\d{4}-\d{2}-\d{2}$/).required(),
  expiryDate: Joi.string().pattern(/^\d{4}-\d{2}-\d{2}$/).required(),
  metadata: Joi.object().optional()
});

const transferSchema = Joi.object({
  proposedOwnerMSP: Joi.string().required()
});

const formulationSchema = Joi.object({
  drugCode: Joi.string().required(),
  unit: Joi.string().required(),
  requirements: Joi.object().pattern(Joi.string(), Joi.number().positive()).required()
});

const productionSchema = Joi.object({
  batchID: Joi.string().required(),
  drugCode: Joi.string().required(),
  outputQuantity: Joi.number().positive().required(),
  unit: Joi.string().required(),
  inputs: Joi.array().items(Joi.object({
    lotId: Joi.string().required(),
    ingredientName: Joi.string().required(),
    amount: Joi.string().required()
  })).required()
});

const validateLotCreate = (req, res, next) => {
  const { error } = lotCreateSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      success: false,
      error: error.details[0].message
    });
  }
  next();
};

const validateTransfer = (req, res, next) => {
  const { error } = transferSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      success: false,
      error: error.details[0].message
    });
  }
  next();
};

const validateFormulation = (req, res, next) => {
  const { error } = formulationSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      success: false,
      error: error.details[0].message
    });
  }
  next();
};

const validateProduction = (req, res, next) => {
  const { error } = productionSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      success: false,
      error: error.details[0].message
    });
  }
  next();
};

module.exports = {
  validateLotCreate,
  validateTransfer,
  validateFormulation,
  validateProduction
};