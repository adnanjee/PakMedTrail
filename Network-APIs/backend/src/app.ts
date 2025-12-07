import express from 'express';
import cors from 'cors';
import authRoutes from './routes/auth';
import lotsRoutes from './routes/lots';
import shipmentsRoutes from './routes/shipments';
import recallsRoutes from './routes/recalls';
import { errorHandler } from './middleware/errorHandler';

export const app = express();

app.use(cors());
app.use(express.json());

app.get('/', (_req, res) => {
  res.json({ status: 'ok', message: 'Fabric API backend is running' });
});

app.use('/auth', authRoutes);
app.use('/api/lots', lotsRoutes);
app.use('/api/shipments', shipmentsRoutes);
app.use('/api/recalls', recallsRoutes);

app.use(errorHandler);
