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
1.1 Crear el Namespace y Operator Group
Crea un archivo llamado aap-namespace.yaml:
YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ansible-automation-platform
---
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: aap-operator-group
  namespace: ansible-automation-platform
spec:
  targetNamespaces:
    - ansible-automation-platform
1.2 Crear la Suscripción al Operador
Crea un archivo llamado aap-subscription.yaml:
YAML
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: ansible-automation-platform-operator
  namespace: ansible-automation-platform
spec:
  channel: stable-2.4
  installPlanApproval: Automatic
  name: ansible-automation-platform-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
1.3 Aplicar manifiestos y verificar instalación
Ejecuta en tu terminal:
Bash
# Crear namespace y suscripción
oc apply -f 01-aap-namespace.yaml
oc apply -f 02-aap-subscription.yaml

# Verificar que el operador esté en estado Succeeded
oc get csv -n ansible-automation-platform
Actividad 2: Despliegue de las Instancias de AAP Controller y EDA Controller
Una vez que el operador está activo, desplegamos las instancias mediante Custom Resources (CRDs).
2.1 Desplegar AAP Controller (AutomationController)
Crea el manifiesto aap-controller.yaml:

2.2 Desplegar EDA Controller (EDA)
Crea el manifiesto 04-eda-controller.yaml:
YAML
apiVersion: eda.ansible.com/v1alpha1
kind: EDA
metadata:
  name: eda-controller
  namespace: ansible-automation-platform
spec:
  replicas: 1
  ingress_type: Route
  automation_server_url: "https://aap-controller-ansible-automation-platform.apps-crc.testing"
2.3 Aplicar y verificar instancias
Bash
oc apply -f 03-aap-controller.yaml
oc apply -f 04-eda-controller.yaml

# Esperar a que los pods de AAP y EDA estén en Running (puede tardar unos minutos)
oc get pods -n ansible-automation-platform -w

# Obtener la URL de las rutas generadas para la consola Web
oc get route -n ansible-automation-platform


Actividad 3: Creación de Playbooks de Auto-Remediación (Self-Healing)
Los playbooks definen la acción exacta de remediación. Crearemos un playbook de escalado dinámico ante saturación de tráfico o consumo en Kafka.
3.1 Playbook: scale_inference_pods.yml
Guarda este archivo en tu repositorio Git de automatización SRE:
YAML
---
- name: Auto-Escalado Dinámico de Microservicios SPEI
  hosts: localhost
  connection: local
  gather_facts: false
  vars:
    target_namespace: "redhat-one-rf-demo"
    deployment_name: "spei-enricher"
    target_replicas: 5

  tasks:
    - name: Notificar inicio de auto-healing
      ansible.builtin.debug:
        msg: "Alerta recibida: Escalando {{ deployment_name }} a {{ target_replicas }} réplicas debido a alta latencia o lag."

    - name: Incrementar réplicas del deployment en OpenShift
      kubernetes.core.k8s:
        state: patched
        kind: Deployment
        name: "{{ deployment_name }}"
        namespace: "{{ target_namespace }}"
        definition:
          spec:
            replicas: "{{ target_replicas }}"

    - name: Confirmar estado de actualización
      ansible.builtin.debug:
        msg: "Escalado de {{ deployment_name }} completado exitosamente en < 10s."
3.2 Playbook: restart_degraded_pod.yml
Playbook para reciclar pods con fallo de memoria o estado degradado:
YAML
---

Client ID
 
785f71ba-e2e6-4b88-aca3-89051b4913f8
Client secret

pOcuAW1oibx97O5RjDDhsfGqYiZyBgDh

REset passwords
785f71ba-e2e6-4b88-aca3-89051b4913f8

XuDeL7V450tsE9kkneYMJCl1gOPpNIXM
- name: Reinicio Controlado de Pods Degradados
  hosts: localhost
  connection: local
  gather_facts: false
  vars:
    target_namespace: "redhat-one-rf-demo"
    pod_label: "app=spei-enricher"

  tasks:
    - name: Eliminar pods degradados para forzar recreación por ReplicaSet
      kubernetes.core.k8s:
        state: absent
        kind: Pod
        namespace: "{{ target_namespace }}"
        label_selectors:
          - "{{ pod_label }}"
Actividad 4: Configuración del Event-Driven Ansible Rulebook (eda-rulebook-sre.yml)
El Rulebook escucha métricas/webhooks (ej. enviadas por Prometheus Alertmanager o Webhook) y dispara los Job Templates en AAP.
Crea el archivo eda-rulebook-sre.yml:
YAML
---
- name: Reglas SRE para Auto-Sanación del Pipeline SPEI
  hosts: all
  sources:
    - ansible.eda.webhook:
        host: 0.0.0.0
        port: 5000

  rules:
    - name: Detectar incremento alto de TPS o Consumer Lag
      condition: event.payload.alert_name == "HighConsumerLagOrTPS"
      action:
        run_job_template:
          name: "SPEI - Auto Scale Inference Pods"
          organization: "Default"
          job_args:
            extra_vars:
              target_replicas: 8

    - name: Detectar degradación de servicio en Pod
      condition: event.payload.alert_name == "PodDegradedError"
      action:
        run_job_template:
          name: "SPEI - Restart Degraded Pod"
          organization: "Default"
Actividad 5: Despliegue del Rulebook Activation Deployment en OpenShift
Para ejecutar el Rulebook de forma nativa en OpenShift, creamos la Deployment en el clúster expuesta con un Service/Route.
Crea el manifiesto 05-eda-rulebook-deployment.yaml:
YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spei-eda-rulebook-sre
  namespace: ansible-automation-platform
  labels:
    app: spei-eda-rulebook
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spei-eda-rulebook
  template:
    metadata:
      labels:
        app: spei-eda-rulebook
    spec:
      containers:
      - name: rulebook-runner
        image: quay.io/ansible/eda-ansible-rulebook:latest
        args:
          - ansible-rulebook
          - --rulebook
          - /env/eda-rulebook-sre.yml
          - -i
          - inventory.yml
          - --verbose
        ports:
        - containerPort: 5000
          name: webhook
        resources:
          limits:
            cpu: "500m"
            memory: "512Mi"
          requests:
            cpu: "100m"
            memory: "256Mi"
---
apiVersion: v1
kind: Service
metadata:
  name: spei-eda-webhook-svc
  namespace: ansible-automation-platform
spec:
  ports:
  - port: 5000
    targetPort: 5000
    name: webhook
  selector:
    app: spei-eda-rulebook
---
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: spei-eda-webhook-route
  namespace: ansible-automation-platform
spec:
  to:
    kind: Service
    name: spei-eda-webhook-svc
  port:
    targetPort: webhook
Aplicar el despliegue del Rulebook:
Bash
oc apply -f 05-eda-rulebook-deployment.yaml
Actividad 6: Simulación y Pruebas del Self-Healing
Puedes simular una alerta SRE enviando un evento cURL directo al Webhook de EDA para validar que el autoscaling se ejecuta en menos de 10 segundos:
Bash
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
