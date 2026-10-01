import type { DomainService } from '../services/domain-service.js';
import type { ProductService } from '../services/product-service.js';
import { createTools } from './tools.js';

export class AgentService {
  constructor(
    private readonly domain: DomainService,
    private readonly product: ProductService,
  ) {}

  async answer(question: string, requestId: string) {
    const lower = question.toLowerCase();
    const tools = createTools(this.domain, this.product);
    let tool: string = 'getFleetOverview';
    let result: unknown;
    const match = question.match(/[0-9a-f]{8}-[0-9a-f-]{27,}/i);
    if (match && lower.includes('risk')) {
      tool = 'getMaintenanceRisk';
      result = await tools.getMaintenanceRisk({ vehicleId: match[0] });
    } else if (match && lower.includes('telemetry')) {
      tool = 'getVehicleTelemetrySummary';
      result = await tools.getVehicleTelemetrySummary({ vehicleId: match[0] });
    } else if (lower.includes('alert')) {
      tool = 'getOpenAlerts';
      result = await tools.getOpenAlerts();
    } else if (lower.includes('analytic') || lower.includes('telemetry')) {
      tool = 'getFleetAnalytics';
      result = await tools.getFleetAnalytics();
    } else result = await tools.getFleetOverview();
    const summary = JSON.stringify(result);
    return {
      answer: `Grounded response from ${tool}: ${summary}`,
      tool,
      requestId,
      auditedAt: new Date().toISOString(),
    };
  }
}
