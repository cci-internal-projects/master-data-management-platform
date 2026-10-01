from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

from airflow.sdk import DAG, task

import django
from django.db import transaction

django.setup()


from zonos_northbound_api.northbound_client import client_v2
from core.models import (
    Device,
    Consumer,
    ServicePoint,
    DeviceInstallation,
    Contract,
    DeviceType,
    DeviceTemplate,
    ElectricalNode,
    GeographicalNode,
    PaymentType,
    ConsumerParameterDefinition,
    ConsumerParameterValue,
    ServicePointParameterDefinition,
    ServicePointParameterValue,
    DeviceParameterDefinition,
    DeviceParameterValue,
    DeviceInstallationParameterDefinition,
    DeviceInstallationParameterValue,
)
from bses_module.models import NonSmToSmJob, States
from bses_module.schemas import NonSmartToSmartRequest


logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
MAX_JOBS_PER_RUN = 100


def ensure_consumer(consumer_id: str, consumer_name: str) -> tuple[Consumer, bool]:
    consumer, created = Consumer.objects.get_or_create(
        consumer_id=consumer_id,
        defaults={
            "consumer_name": consumer_name,
        },
    )
    if created:
        logger.info(f"Consumer {consumer_id} created")
    else:
        logger.info(f"Consumer {consumer_id} already exists")
    return (consumer, created)


def create_service_point(
    service_point_id: str,
    electrical_node: ElectricalNode,
    geographical_node: GeographicalNode,
) -> ServicePoint:
    service_point = ServicePoint.objects.create(
        id=service_point_id,
        electrical_node=electrical_node,
        geographical_node=geographical_node,
    )
    logger.info(f"Service point {service_point_id} created")
    return service_point


def ensure_device(
    device_id: str, device_type: DeviceType, device_template: DeviceTemplate
) -> tuple[Device, bool]:
    device, created = Device.objects.get_or_create(
        device_id=device_id,
        defaults={
            "device_type": device_type,
            "device_template": device_template,
        },
    )
    if created:
        logger.info(f"Device {device_id} created")
    else:
        logger.info(f"Device {device_id} already exists")
    return (device, created)


def create_contract(
    service_point: ServicePoint, consumer: Consumer, start_date: datetime, payment_type: PaymentType
) -> Contract:
    contract = Contract.objects.create(
        consumer=consumer,
        service_point=service_point,
        start_date=start_date,
        is_active=True,
        payment_type=payment_type,
    )
    logger.info(f"Contract {contract.id} created")
    return contract


def create_device_installation(
    device: Device,
    service_point: ServicePoint,
    is_active: bool,
    start_date: datetime,
    end_date: datetime | None = None,
):
    device_installation = DeviceInstallation.objects.create(
        device=device,
        service_point=service_point,
        is_active=is_active,
        start_date=start_date,
        end_date=end_date,
    )
    logger.info(f"Device installation {device_installation.id} created")
    return device_installation


def bulk_set_consumer_parameters(
    consumer: Consumer, parameter_config: dict, parameter_definitions: dict
) -> None:
    ConsumerParameterValue.objects.bulk_create(
        [
            ConsumerParameterValue(
                consumer=consumer,
                parameter=parameter_definitions[name],
                value=value,
            )
            for name, value in parameter_config.items()
            if name in parameter_definitions
        ]
    )


def bulk_set_service_point_parameters(
    service_point: ServicePoint, parameter_config: dict, parameter_definitions: dict
) -> None:
    ServicePointParameterValue.objects.bulk_create(
        [
            ServicePointParameterValue(
                service_point=service_point,
                parameter=parameter_definitions[name],
                value=value,
            )
            for name, value in parameter_config.items()
            if name in parameter_definitions
        ]
    )


def bulk_set_device_parameters(
    device: Device, parameter_config: dict, parameter_definitions: dict
) -> None:
    DeviceParameterValue.objects.bulk_create(
        [
            DeviceParameterValue(
                device=device,
                parameter=parameter_definitions[name],
                value=value,
            )
            for name, value in parameter_config.items()
            if name in parameter_definitions
        ]
    )


def bulk_set_sm_device_installation_parameters(
    device_installation: DeviceInstallation, parameter_config: dict, parameter_definitions: dict
) -> None:
    DeviceInstallationParameterValue.objects.bulk_create(
        [
            DeviceInstallationParameterValue(
                device_installation=device_installation,
                parameter=parameter_definitions[name],
                start_value=value,
            )
            for name, value in parameter_config.items()
            if name in parameter_definitions
        ]
    )


def bulk_set_non_sm_device_installation_parameters(
    device_installation: DeviceInstallation, parameter_config: dict, parameter_definitions: dict
) -> None:
    DeviceInstallationParameterValue.objects.bulk_create(
        [
            DeviceInstallationParameterValue(
                device_installation=device_installation,
                parameter=parameter_definitions[name],
                end_value=value,
            )
            for name, value in parameter_config.items()
            if name in parameter_definitions
        ]
    )


with DAG(
    dag_id="BSES-non-SM-to-SM",
    start_date=datetime(2025, 1, 1, tzinfo=IST),
    schedule="*/2 * * * *",
    catchup=False,
) as dag:

    @task.short_circuit
    def get_jobs() -> list[str]:
        # Claim a bounded batch atomically so overlapping scheduler runs do not
        # dispatch the same rows, and rows outside this batch remain READY.
        with transaction.atomic():
            job_ids = list(
                NonSmToSmJob.objects.filter(job_status=States.READY)
                .order_by("job_created_at", "job_id")
                .select_for_update(skip_locked=True)
                .values_list("job_id", flat=True)[:MAX_JOBS_PER_RUN]
            )
            if job_ids:
                updated_jobs = NonSmToSmJob.objects.filter(
                    job_id__in=job_ids, job_status=States.READY
                ).update(job_status=States.IN_PROGRESS)
                logger.info("Claimed %s of %s READY jobs", updated_jobs, len(job_ids))
            else:
                logger.info("No READY jobs found")
        return job_ids

    @task(
        retries=0,
        retry_delay=timedelta(minutes=5),
        doc_md="""
            Task 1
                - ensure consumer
                - ensure service point
                - ensure sm_device
                - ensure non_sm_device
                - create contract
                - create sm_device installation
                - create non_sm_device uninstallation
                - else rollback
        """,
    )
    def task_1(job_id: str) -> dict:
        job_obj = NonSmToSmJob.objects.only(
            "job_id", "sm_device_id", "consumer_id", "payload", "job_message"
        ).get(job_id=job_id)
        sm_device_id = job_obj.sm_device_id
        consumer_id = job_obj.consumer_id
        job_obj.job_message = f"{job_obj.job_message}\n Processing task 1"
        job_obj.save(update_fields=["job_message", "job_run_at"])

        try:
            body = NonSmartToSmartRequest.model_validate(job_obj.payload)
            with transaction.atomic():
                geographical_node = GeographicalNode.objects.get(name="Root")
                electrical_node = ElectricalNode.objects.get(name="Root")

                # Create or get consumer
                consumer, created = ensure_consumer(
                    consumer_id=consumer_id, consumer_name=body.consumerMaster.consumerName
                )

                # Create Service Point
                if created:
                    logger.info(f"Consumer {consumer_id} created")
                    service_point_id = f"{body.consumerMaster.accountId}_SP1"
                    service_point = create_service_point(
                        service_point_id=service_point_id,
                        electrical_node=electrical_node,
                        geographical_node=geographical_node,
                    )
                else:
                    logger.info(f"Consumer {consumer_id} already exists")
                    contracts = Contract.objects.filter(consumer=consumer)
                    service_point_id = f"{body.consumerMaster.accountId}_SP{contracts.count() + 1}"
                    service_point = create_service_point(
                        service_point_id=service_point_id,
                        electrical_node=electrical_node,
                        geographical_node=geographical_node,
                    )

                # Create Contract
                contract = create_contract(
                    service_point=service_point,
                    consumer=consumer,
                    start_date=body.timestamp,
                    payment_type=PaymentType.PREPAID
                    if body.newMeterDetails.prepaidpostpaidflag == True
                    else PaymentType.POSTPAID,
                )

                consumer_parameters = body.consumerMaster.model_dump(exclude={"accountId"})
                sm_device_parameters = body.newMeterDetails.model_dump(exclude={"metersrno"})
                sm_installation_parameters = sm_device_parameters
                old_device_parameters = (
                    body.oldMeterDetails.model_dump(exclude={"metersrno"})
                    if body.oldMeterDetails
                    else {}
                )
                old_installation_parameters = old_device_parameters

                consumer_parameter_definitions = {
                    parameter.name: parameter
                    for parameter in ConsumerParameterDefinition.objects.filter(
                        name__in=consumer_parameters
                    )
                }
                service_point_parameter_definitions = {
                    parameter.name: parameter
                    for parameter in ServicePointParameterDefinition.objects.filter(
                        name__in=consumer_parameters
                    )
                }
                device_parameter_names = set(sm_device_parameters) | set(old_device_parameters)
                device_parameter_definitions = {
                    parameter.name: parameter
                    for parameter in DeviceParameterDefinition.objects.filter(
                        name__in=device_parameter_names
                    )
                }
                installation_parameter_names = set(sm_installation_parameters) | set(
                    old_installation_parameters
                )
                installation_parameter_definitions = {
                    parameter.name: parameter
                    for parameter in DeviceInstallationParameterDefinition.objects.filter(
                        name__in=installation_parameter_names
                    )
                }

                # Set consumer parameters
                bulk_set_consumer_parameters(
                    consumer=consumer,
                    parameter_config=consumer_parameters,
                    parameter_definitions=consumer_parameter_definitions,
                )
                # Set service point parameters
                bulk_set_service_point_parameters(
                    service_point=service_point,
                    parameter_config=consumer_parameters,
                    parameter_definitions=service_point_parameter_definitions,
                )

                sm_device_type_name = f"{body.newMeterDetails.metermake}_{body.newMeterDetails.meterphase}_{body.newMeterDetails.metercategory}"
                sm_device_template_name = f"{body.newMeterDetails.metermake}_{body.newMeterDetails.meterphase}_{body.newMeterDetails.metercategory}"
                # Get SM Device Type and Template
                sm_device_type = DeviceType.objects.get(name=sm_device_type_name)
                sm_device_template = DeviceTemplate.objects.get(name=sm_device_template_name)

                # Create SM device
                sm_device, created = ensure_device(
                    device_id=sm_device_id,
                    device_type=sm_device_type,
                    device_template=sm_device_template,
                )

                # Set SM device parameters
                bulk_set_device_parameters(
                    device=sm_device,
                    parameter_config=sm_device_parameters,
                    parameter_definitions=device_parameter_definitions,
                )

                # Create SM device installation
                sm_device_installation = create_device_installation(
                    device=sm_device,
                    service_point=service_point,
                    is_active=True,
                    start_date=body.timestamp,
                )

                # Set parameters for sm device installation
                bulk_set_sm_device_installation_parameters(
                    device_installation=sm_device_installation,
                    parameter_config=sm_installation_parameters,
                    parameter_definitions=installation_parameter_definitions,
                )

                # Get non sm device type and template
                non_sm_device_type = DeviceType.objects.get(
                    id="9c423eb7-e2dc-4a66-b82f-409be8bb4268"
                )
                non_sm_device_template = DeviceTemplate.objects.get(
                    id="705c53bc-5de5-4112-b80f-97a87dac3756"
                )

                if body.oldMeterDetails:
                    non_sm_device_id = (
                        f"{body.oldMeterDetails.metermake}{body.oldMeterDetails.metersrno}"
                    )
                    # Create or get non SM device
                    non_sm_device, created = ensure_device(
                        device_id=non_sm_device_id,
                        device_type=non_sm_device_type,
                        device_template=non_sm_device_template,
                    )

                    # Set parameters for non sm device
                    bulk_set_device_parameters(
                        device=non_sm_device,
                        parameter_config=old_device_parameters,
                        parameter_definitions=device_parameter_definitions,
                    )

                    # Un-install old non_sm_device
                    non_sm_device_installation = create_device_installation(
                        device=non_sm_device,
                        service_point=service_point,
                        is_active=False,
                        start_date=datetime(1970, 1, 1, tzinfo=IST),
                        end_date=body.timestamp,
                    )

                    # Set parameters for non sm device uninstallation
                    bulk_set_non_sm_device_installation_parameters(
                        device_installation=non_sm_device_installation,
                        parameter_config=old_installation_parameters,
                        parameter_definitions=installation_parameter_definitions,
                    )
                job_obj.service_point_id = service_point.id
                job_obj.job_message = f"{job_obj.job_message}\n Task 1 MDM Asset creation completed"
                job_obj.save(update_fields=["service_point_id", "job_message", "job_run_at"])
            logger.info("Task 1 completed successfully for job %s", job_obj.job_id)
            return {"job_id": job_obj.job_id, "service_point_id": service_point_id}
        except Exception as e:
            logger.error(f"Error in task_1: {e}")
            job_obj.job_status = States.FAILED
            job_obj.job_message = str(e)
            job_obj.save(update_fields=["job_status", "job_message", "job_run_at"])
            raise e

    @task(
        retries=5,
        retry_delay=timedelta(minutes=5),
        doc_md="""
        Invoke zonos northbound api
            - Create customer
            - Create metering point
            - Set metering point parameters
            - Create device with device parameters
        """,
    )
    def task_2(job: dict) -> dict:
        job_obj = NonSmToSmJob.objects.only(
            "job_id", "consumer_id", "sm_device_id", "payload", "job_message", "job_status"
        ).get(job_id=job["job_id"])

        job_obj.job_message = f"{job_obj.job_message}\n Processing task 2"
        job_obj.save(update_fields=["job_message", "job_run_at"])

        body: NonSmartToSmartRequest = NonSmartToSmartRequest.model_validate(job_obj.payload)
        group_uuid = "e327f3e7-774d-48e2-b44a-f26e5b3a9434"
        # Create customer
        try:
            client_v2.createCustomer(
                customerId=job_obj.consumer_id,
                language="en",
                timeZone="Asia/Kolkata",
                typeof="unknown",
            )
        except Exception as e:
            logger.error(f"Customer creation error in task_2: {e}")
            job_obj.job_status = States.FAILED
            job_obj.job_message = (
                f"{job_obj.job_message} > zonos customer creation failed ({repr(e)})"
            )
            job_obj.save(update_fields=["job_status", "job_message", "job_run_at"])
            raise e

        # Create Metering Point
        try:
            client_v2.createMeteringPoint(
                meteringPointId=job["service_point_id"],
                groupUuid=group_uuid,
                latitude=body.consumerMaster.latitude,
                longitude=body.consumerMaster.longitude,
            )
        except Exception as e:
            logger.error(f"Metering Point creation error in task_2: {e}")
            job_obj.job_status = States.FAILED
            job_obj.job_message = (
                f"{job_obj.job_message} > zonos metering point creation failed ({repr(e)})"
            )
            job_obj.save(update_fields=["job_status", "job_message", "job_run_at"])
            raise e

        # Set Metering Point parameters
        try:
            metering_point_parameters = {
                f"ext.{key}": value
                for key, value in body.consumerMaster.model_dump(
                    exclude={"meterinstalldate", "latitude", "longitude", "meterStatus"},
                    mode="json",
                ).items()
            }

            response = client_v2.bulkSetMeteringPointParameters(
                meteringPoint=job["service_point_id"],
                parameters=metering_point_parameters,
            )
            logger.info(f"Metering Point parameters set response: {response}")
        except Exception as e:
            logger.error(f"Metering Point parameters set error in task_2: {e}")
            job_obj.job_status = States.FAILED
            job_obj.job_message = (
                f"{job_obj.job_message} > zonos metering point parameters set failed ({repr(e)})"
            )
            job_obj.save(update_fields=["job_status", "job_message", "job_run_at"])
            raise e

        # Create Device
        try:
            device_id = job_obj.sm_device_id
            device_type_template_name = f"{body.newMeterDetails.metermake}_{body.newMeterDetails.meterphase}_{body.newMeterDetails.metercategory}"
            logger.info(f"Device type template name: {device_type_template_name}")
            device_type_uuid = str(DeviceType.objects.get(name=device_type_template_name).id)
            device_template_uuid = str(
                DeviceTemplate.objects.get(name=device_type_template_name).id
            )

            logger.info(f"Device type uuid: {device_type_uuid}")
            logger.info(f"Device template uuid: {device_template_uuid}")
            logger.info(f"Device id: {device_id}")
            logger.info(f"Communication id: {device_id}")
            logger.info(f"Group uuid: {group_uuid}")
            logger.info(f"Store data: {True}")

            device_parameters = {
                f"ext.{key}": value
                for key, value in body.newMeterDetails.model_dump(
                    exclude={"metersrno"}, mode="json"
                ).items()
            }
            device_parameters["ext.servicepointid"] = job["service_point_id"]

            device: dict = {
                "id": device_id,
                "communicationId": device_id,
                "groupId": group_uuid,
                "typeId": device_type_uuid,
                "templateId": device_template_uuid,
                "model": body.newMeterDetails.meterphase,
                "manufacturer": body.newMeterDetails.metermake,
                "description": "",
                "inventoryState": "installed",
                "managementState": "unknown",
                "dispatchGroup": "",
                "storeData": True,
                "parentId": None,
                "configuration": device_parameters,
            }

            response = client_v2.createDevice(device=device)
            logger.info(f"Device creation response: {response}")

        except Exception as e:
            logger.error(f"Device creation error in task_2: {e}")
            job_obj.job_status = States.FAILED
            job_obj.job_message = (
                f"{job_obj.job_message} > zonos device creation failed ({repr(e)})"
            )
            job_obj.save(update_fields=["job_status", "job_message", "job_run_at"])
            raise e

        job_obj.job_status = States.COMPLETED
        job_obj.job_message = f"{job_obj.job_message}\n Task 2 Zonos Asset creation completed"
        job_obj.save(update_fields=["job_status", "job_message", "job_run_at"])
        return job

    job_ids = get_jobs()
    mdm_assets = task_1.expand(job_id=job_ids)
    task_2.expand(job=mdm_assets)
