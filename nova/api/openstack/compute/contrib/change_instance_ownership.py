# Copyright 2013 IBM Corp.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import webob.exc

from nova.api.openstack import extensions
from nova import db
from nova import exception
from nova import quota
from nova.openstack.common.gettextutils import _


authorize = extensions.extension_authorizer('compute', 'change_instance_ownership')
QUOTAS = quota.QUOTAS

class ChangeInstanceOwnershipController(object):

    def action(self, req, id, body):

        context = req.environ['nova.context']
        authorize(context)
        num_instances = 1

        if 'user_id' in body or 'project_id' in body:
            if 'project_id' in body:
               instance = db.instance_get_by_uuid(context, id)
               project_id = body['project_id']
               instance_project_id = instance.project_id
               if instance_project_id != body['project_id']:
                   req_cores = instance.vcpus
                   req_ram = instance.memory_mb
                   req_fixed_ips = 1
                   reservations = self._create_reservation(context,
                           instances=num_instances,
                           cores=req_cores,
                           ram=req_ram,
                           fixed_ips=req_fixed_ips,
                           project_id=project_id)

                   reservations_del = self._create_reservation(context,
                           instances=-num_instances,
                           cores=-req_cores,
                           ram=-req_ram,
                           fixed_ips=-req_fixed_ips,
                           project_id=instance_project_id)

                   try:
                       QUOTAS.commit(context, reservations,
                                     project_id=project_id)
                       self._update_instance(context, id, body)
                   except Exception:
                       instance_dict = {}
                       instance_dict['project_id'] = instance_project_id
                       self._update_instance(context, id, instance_dict)
                       QUOTAS.rollback(context, reservations)
                       QUOTAS.rollback(context, reservations_del)
                       msg = _("Error while updating instances.")
                       LOG.debug(msg)
                       raise webob.exc.HTTPNotFound(explanation=msg)
                   else:
                       QUOTAS.commit(context, reservations_del)
        else:
            raise webob.exc.HTTPBadRequest(
                explanation="User_id or Project_id were not found in the request body")

    def _update_instance(self, context, instance_uuid, body):
        try:
            db.instance_update(context, instance_uuid, body, update_cells=True)
        except Exception:
            msg = _("Could not update the instance")
            LOG.debug(msg)
            raise webob.exc.HTTPNotFound(explanation=msg)

        return webob.exc.HTTPAccepted()

    def _create_reservation(self, context, instances, cores, ram, fixed_ips,
                            project_id):
         try:
             reservations = QUOTAS.reserve(context,
                                           instances=instances,
                                           cores=cores,
                                           ram=ram,
                                           fixed_ips=fixed_ips,
                                           project_id=project_id)

             return reservations
         except exception.OverQuota as exc:
             quotas = exc.kwargs['quotas']
             overs = exc.kwargs['overs']
             headroom = exc.kwargs['headroom']
             allowed = headroom['instances']
             resource = overs[0]
             used = quotas[resource] - headroom[resource]
             total_allowed = used + headroom[resource]
             requested = dict(num_instances=instances,
                              cores=cores,
                              ram=ram)
             raise exception.TooManyInstances(overs=overs,
                                              req=requested[resource],
                                              used=used,
                                              allowed=total_allowed,
                                              resource=resource)


class Change_instance_ownership(extensions.ExtensionDescriptor):
    """Change instance ownership support."""

    name = "ChangeInstanceOwnership"
    alias = "os-change-instance-ownership"
    namespace = "http://docs.openstack.org/compute/ext/change-instance-ownership/api/v2"
    updated = "2014-03-07T18:00:00-03:00"

    def get_resources(self):
        member_actions = {'action': 'PUT'}
        resources = []
        resource = extensions.ResourceExtension('os-change-instance-ownership',
                                                ChangeInstanceOwnershipController(),
                                                member_actions=member_actions)
        resources.append(resource)
        return resources
