Arquitectura de Instalación
  +-------------------------------------------------------------------------+
  |                          NAMESPACE: ansible-automation-platform         |
  |                                                                         |
  |  +--------------------------------+   +------------------------------+  |
  |  | AAP Operator (AAPController)   |   | EDA Controller               |  |
  |  | (Ejecuta Playbooks SRE)        |   | (Procesa eventos en vivo)    |  |
  |  +---------------+----------------+   +--------------+---------------+  |
  +------------------|-----------------------------------|------------------+
                     | API Webhook / Token               |
                     v                                   v
  +-------------------------------------------------------------------------+
  |  [Rulebook CRD / Pod] <--- Alertas/Métricas <--- [Prometheus / Grafana] |
  |  Evalúa la regla y dispara el Playbook en < 10 segundos               |
  +-------------------------------------------------------------------------+
Actividad 1: Instalación del Operador AAP y EDA Controller
oc apply -f aap-namespace.yaml
oc apply -f aap-subscription.yaml

# Verificar que el operador esté en estado Succeeded
oc get csv -n ansible-automation-platform
Actividad 2: Despliegue de las Instancias de AAP Controller y EDA Controller
Una vez que el operador está activo, desplegamos las instancias mediante Custom Resources (CRDs).
2.1 Desplegar AAP Controller (AutomationController)
Crea el manifiesto aap-controller.yaml:

2.2 Desplegar EDA Controller (EDA)
Crea el manifiesto eda-controller.yaml:

2.3 Aplicar y verificar instancias
oc apply -f aap-controller.yaml
oc apply -f eda-controller.yaml

# Esperar a que los pods de AAP y EDA estén en Running (puede tardar unos minutos)
oc get pods -n ansible-automation-platform -w

# Obtener la URL de las rutas generadas para la consola Web
oc get route -n ansible-automation-platform


Actividad 3: Creación de Playbooks de Auto-Remediación (Self-Healing)
3.1 Playbook: scale_inference_pods.yml

3.2 Playbook: restart_degraded_pod.yml

Actividad 4: Configuración del Event-Driven Ansible Rulebook (eda-rulebook-sre.yml)
Crea el archivo eda-rulebook-sre.yml:

Actividad 5: Despliegue del Rulebook Activation Deployment en OpenShift
Crea el manifiesto eda-rulebook-deployment.yaml:

oc apply -f eda-rulebook-deployment.yaml


Actividad 6: Simulación y Pruebas del Self-Healing
Puedes simular una alerta SRE enviando un evento cURL directo al Webhook de EDA para validar que el autoscaling se ejecuta en menos de 10 segundos:

# 1. Obtener la URL pública del webhook de EDA
EDA_ROUTE=$(oc get route spei-eda-webhook-route -n ansible-automation-platform -o jsonpath='{.spec.host}')

# 2. Simular un evento de alerta enviando un payload JSON
curl -X POST http://$EDA_ROUTE/endpoint \
     -H "Content-Type: application/json" \
     -d '{"alert_name": "HighConsumerLagOrTPS", "severity": "critical"}'

# 3. Validar el escalado automático inmediato del microservicio spei-enricher
oc get pods -n redhat-one-rf-demo -l app=spei-enricher -w



WffqvijZ1v5mokUSt7Ww4BTsuQHx3p


https://aap-controller-ansible-automation-platform.apps-crc.testing


SPEI-Self-Healing-Activation
oc describe rulebookactivation SPEI-Self-Healing-Activation -n ansible-automation-platform


oc get pods -n ansible-automation-platform | grep eda

oc describe pod  eda-server-operator-controller-manager-68d97fcfbb-lt64zj -n ansible-automation-platform


HxwTqM32YgvqneusmrGoGEmDYnCN3G



admin: f306UekB5RrQrStFrCgVl0m2RqhHfj4k
