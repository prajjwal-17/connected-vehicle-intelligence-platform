import { createHash } from 'node:crypto';
import { PrismaClient, type Prisma } from '@prisma/client';

const prisma = new PrismaClient();

function deterministicId(key: string): string {
  const hex = createHash('sha256').update(`fleetpulse-seed:${key}`).digest('hex').slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20)}`;
}

async function main() {
  const tenants = [
    { key: 'northstar', name: 'Northstar Logistics' },
    { key: 'bluebird', name: 'Bluebird Mobility' },
  ];

  for (const tenantInput of tenants) {
    const tenant = await prisma.tenant.upsert({
      where: { id: deterministicId(`tenant:${tenantInput.key}`) },
      update: { name: tenantInput.name },
      create: { id: deterministicId(`tenant:${tenantInput.key}`), name: tenantInput.name },
    });

    const user = await prisma.user.upsert({
      where: { id: deterministicId(`user:${tenantInput.key}:manager`) },
      update: {
        email: `manager@${tenantInput.key}.fleetpulse.test`,
        name: `${tenant.name} Manager`,
        role: 'FLEET_MANAGER',
      },
      create: {
        id: deterministicId(`user:${tenantInput.key}:manager`),
        tenantId: tenant.id,
        email: `manager@${tenantInput.key}.fleetpulse.test`,
        name: `${tenant.name} Manager`,
        role: 'FLEET_MANAGER',
      },
    });

    const fleetCount = tenantInput.key === 'northstar' ? 2 : 1;
    for (let fleetIndex = 1; fleetIndex <= fleetCount; fleetIndex += 1) {
      const fleet = await prisma.fleet.upsert({
        where: { id: deterministicId(`fleet:${tenantInput.key}:${fleetIndex}`) },
        update: { name: `${tenant.name} Fleet ${fleetIndex}` },
        create: {
          id: deterministicId(`fleet:${tenantInput.key}:${fleetIndex}`),
          tenantId: tenant.id,
          name: `${tenant.name} Fleet ${fleetIndex}`,
          description: `Synthetic development fleet ${fleetIndex}`,
        },
      });

      const drivers = [];
      for (let driverIndex = 1; driverIndex <= 3; driverIndex += 1) {
        drivers.push(
          await prisma.driver.upsert({
            where: {
              id: deterministicId(`driver:${tenantInput.key}:${fleetIndex}:${driverIndex}`),
            },
            update: { name: `Driver ${fleetIndex}-${driverIndex}` },
            create: {
              id: deterministicId(`driver:${tenantInput.key}:${fleetIndex}:${driverIndex}`),
              fleetId: fleet.id,
              name: `Driver ${fleetIndex}-${driverIndex}`,
            },
          }),
        );
      }

      for (let vehicleIndex = 1; vehicleIndex <= 8; vehicleIndex += 1) {
        const vehicle = await prisma.vehicle.upsert({
          where: {
            id: deterministicId(`vehicle:${tenantInput.key}:${fleetIndex}:${vehicleIndex}`),
          },
          update: { status: vehicleIndex === 8 ? 'MAINTENANCE' : 'ACTIVE' },
          create: {
            id: deterministicId(`vehicle:${tenantInput.key}:${fleetIndex}:${vehicleIndex}`),
            fleetId: fleet.id,
            vin: `FP${tenantInput.key.slice(0, 2).toUpperCase()}${String(fleetIndex).padStart(2, '0')}${String(vehicleIndex).padStart(2, '0')}000000000`,
            oem: vehicleIndex % 2 === 0 ? 'Volta Motors' : 'Northwind Automotive',
            model: vehicleIndex % 2 === 0 ? 'Eon Cargo' : 'Atlas Van',
            modelYear: 2022 + (vehicleIndex % 3),
            powertrainType:
              vehicleIndex % 3 === 0 ? 'EV' : vehicleIndex % 2 === 0 ? 'HYBRID' : 'ICE',
            status: vehicleIndex === 8 ? 'MAINTENANCE' : 'ACTIVE',
          },
        });

        const currentDriver = drivers[(vehicleIndex - 1) % drivers.length];
        await prisma.vehicleDriverAssignment.upsert({
          where: {
            vehicleId_driverId_assignedAt: {
              vehicleId: vehicle.id,
              driverId: currentDriver.id,
              assignedAt: new Date('2025-01-01T00:00:00.000Z'),
            },
          },
          update: { unassignedAt: null },
          create: {
            id: deterministicId(
              `assignment:${tenantInput.key}:${fleetIndex}:${vehicleIndex}:current`,
            ),
            vehicleId: vehicle.id,
            driverId: currentDriver.id,
            assignedAt: new Date('2025-01-01T00:00:00.000Z'),
          },
        });

        const maintenanceData: Prisma.MaintenanceRecordCreateInput = {
          id: deterministicId(`maintenance:${tenantInput.key}:${fleetIndex}:${vehicleIndex}`),
          maintenanceType: vehicleIndex === 8 ? 'Brake inspection' : 'Scheduled service',
          description:
            vehicleIndex === 8 ? 'Synthetic brake wear inspection' : 'Synthetic routine service',
          performedAt: new Date('2025-02-15T00:00:00.000Z'),
          cost: vehicleIndex === 8 ? 840 : 325,
          odometer: 25000 + vehicleIndex * 700,
          vehicle: { connect: { id: vehicle.id } },
        };
        await prisma.maintenanceRecord.upsert({
          where: { id: maintenanceData.id },
          update: maintenanceData,
          create: maintenanceData,
        });

        const alertData: Prisma.AlertCreateInput = {
          id: deterministicId(`alert:${tenantInput.key}:${fleetIndex}:${vehicleIndex}`),
          alertType: vehicleIndex === 8 ? 'BRAKE_WEAR' : 'SERVICE_DUE',
          severity: vehicleIndex === 8 ? 'CRITICAL' : 'INFO',
          status: vehicleIndex === 8 ? 'OPEN' : 'RESOLVED',
          detectedAt: new Date('2025-03-01T00:00:00.000Z'),
          resolvedAt: vehicleIndex === 8 ? null : new Date('2025-03-02T00:00:00.000Z'),
          metadata: { source: 'synthetic-seed', vehicleIndex },
          vehicle: { connect: { id: vehicle.id } },
        };
        await prisma.alert.upsert({
          where: { id: alertData.id },
          update: alertData,
          create: alertData,
        });
      }

      await prisma.auditLog.upsert({
        where: { id: deterministicId(`audit:${tenantInput.key}:${fleetIndex}`) },
        update: { metadata: { source: 'synthetic-seed', fleetId: fleet.id } },
        create: {
          id: deterministicId(`audit:${tenantInput.key}:${fleetIndex}`),
          tenantId: tenant.id,
          actorId: user.id,
          action: 'FLEET_SEEDED',
          resourceType: 'Fleet',
          resourceId: fleet.id,
          metadata: { source: 'synthetic-seed' },
        },
      });
    }
  }
}

main()
  .then(async () => prisma.$disconnect())
  .catch(async (error) => {
    console.error(error);
    await prisma.$disconnect();
    process.exit(1);
  });
